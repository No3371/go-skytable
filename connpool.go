package skytable

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

// ConnPool manage multiple Conns automatically.
//
// A conn will be spawned or taken from a internal queue (channel) to perform the task for most of the methods, and be queued back when done.
// A slow start should be expected if bursting packets with a new pool or not yet used to send a burst of packets.
//
// Therefore, `prewarming` by spawning a burst of parallel packet-sending goroutines is viable.
type ConnPool struct {
	available chan *Conn
	opened int64 // atomic
	remote       *net.TCPAddr
	opts ConnPoolOptions
}

type ConnPoolOptions struct {
	Cap          int64 // The maximun of opened Conns at the same time
	AuthProvider func() (username, token string) // Do not keep auth info in memory
	DefaultEntity string // "KEYSPACE" or "KEYSPACE:CONTAINER"
}

var DefaultConnPoolOptions = ConnPoolOptions{
	Cap: int64(runtime.NumCPU()) * 2,
}

// NewConnPool create a ConnPool that manage Conns automatically.
// DefaultConnPoolOptions is available for the `opts` argument.
func NewConnPool(remote *net.TCPAddr, opts ConnPoolOptions) *ConnPool {
	if opts.Cap == 0 {
		opts.Cap = int64(runtime.NumCPU()) * 2
	}

	return &ConnPool{
		opened:       0,
		available:    make(chan *Conn, opts.Cap),
		remote:       remote,
		opts: opts,
	}
}

func (c *ConnPool) OpenedConns() int64 {
	return atomic.LoadInt64(&c.opened)
}

func (c *ConnPool) popConn(dontOpenNew bool) (conn *Conn, err error) {
	if dontOpenNew {
		return <-c.available, nil
	}

	select {
	case conn = <-c.available:
		return conn, nil
	default:
		if atomic.LoadInt64(&c.opened) < c.opts.Cap {
			return c.openConn()
		} else {
			conn = <-c.available
			return conn, nil
		}
	}
}

func (c *ConnPool) pushConn(conn *Conn) {
	select {
	case <-conn.closed:
		atomic.AddInt64(&c.opened, -1)
		return
	default:
	}

	select {
	case c.available <- conn:
	default:
		go func() {
			c.available <- conn
		}()
	}
}

func (c *ConnPool) openConn() (conn *Conn, err error) {
	if c.opts.AuthProvider != nil {
		conn, err = NewConnAuth(c.remote, c.opts.AuthProvider)
		if err != nil {
			return nil, fmt.Errorf("conn pool failed to open new conn: %w", err)
		}
	} else {
		conn, err = NewConn(c.remote)
		if err != nil {
			return nil, fmt.Errorf("conn pool failed to open new conn: %w", err)
		}
	}

	pv, err := conn.SysInfoProtocol(context.Background())
	if err != nil {
		return nil, fmt.Errorf("conn pool: conn: failed to get protocol version: %w", err)
	}

	if pv != ProtoVer {
		return nil, protocol.ErrProtocolVersion
	}

	if c.opts.DefaultEntity != "" {
		err = conn.Use(context.Background(), c.opts.DefaultEntity)
		if err != nil {
			return nil, fmt.Errorf("conn pool: conn: failed to USE default entity: %w", err)
		}
	}


	atomic.AddInt64(&c.opened, 1)
	return conn, nil
}

// DoEachConn execute the supplied func for every conn opened before the call.
// If an error is returned, the iteration may be incomplete.
func (c *ConnPool) DoEachConn(action func (conn *Conn) error) error {
	t := time.Now()
	ited := 0
	conns := make([]*Conn, 0, c.OpenedConns())
	defer func () {
		for _, conn := range conns {
			c.pushConn(conn)
		}
	} ()

	for ; ited < int(c.OpenedConns()); {

		conn, err := c.popConn(true)
		if err != nil {
			return err
		}

		if conn.openedAt.After(t) {
			continue
		}

		err = action(conn)
		if err != nil {
			return err
		}

		ited++
		conns = append(conns, conn)
	}


	return nil
}

func (c *ConnPool) Heya(ctx context.Context, echo string) (err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Heya(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Heya(ctx, echo)
}

func (c *ConnPool) AuthLogin(ctx context.Context, username string, token string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.AuthLogin(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthLogin(ctx, username, token)
}

func (c *ConnPool) Exists(ctx context.Context, keys []string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.Exists(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Exists(ctx, keys)
}

func (c *ConnPool) Del(ctx context.Context, keys []string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.Del(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Del(ctx, keys)
}

func (c *ConnPool) Get(ctx context.Context, key string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.Get(): %w", err)
	}
	defer c.pushConn(conn)

	return c.Get(ctx, key)
}

// GetString() is a strict version of Get() that only success if the value is stored as String in Skytable.
func (c *ConnPool) GetString(ctx context.Context, key string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.GetString(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.GetString(ctx, key)
}

// GetBytes() is a strict version of GET that only success if the value is stored as BinaryString in Skytable.
func (c *ConnPool) GetBytes(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.GetBytes(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.GetBytes(ctx, key)
}

func (c *ConnPool) MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MGet(ctx, keys)
}

func (c *ConnPool) Set(ctx context.Context, key string, value any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Set(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Set(ctx, key, value)
}

func (c *ConnPool) Update(ctx context.Context, key string, value any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Update(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Update(ctx, key, value)
}

// func (c *ConnPool) UpdateString(ctx context.Context, key string, value string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) UpdateBytes(ctx context.Context, key string, value []byte) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) Pop(ctx context.Context, key string) (protocol.DataType, any, error) {
// 	panic("not implemented") // TODO: Implement
// }

func (c *ConnPool) Exec(ctx context.Context, packet *QueryPacket) ([]response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.Exec(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Exec(ctx, packet)
}

func (c *ConnPool) ExecSingleRawQuery(segments ...string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.ExecSingleRawQuery(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.ExecSingleRawQuery(segments...)
}

// func (c *ConnPool) ExecRawQuery(actions ...string) (any, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) InspectKeyspaces(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) ListAllKeyspaces(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) CreateKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

func (c *ConnPool) CreateKeyspace(ctx context.Context, path string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.CreateKeyspace(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.CreateKeyspace(ctx, path)
}

func (c *ConnPool) DropKeyspace(ctx context.Context, path string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Dropkeyspace(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.DropKeyspace(ctx, path)
}

// *Conn.Use() is for sending "USE KEYSPACE" or "USE KEYSPACE:TABLE", which change the container the connection is using.
//
// This method will take all conns and do *Conn.Use() on each, and overwrite the DefaultEntity of the pool.
//
// Noted that if there's an error, it's possible that the iteration is not completed and the connections may be using different containers.
// So it's suggested to reset them by doing DDLs not likely to go wrong, like Use("default").
func (c *ConnPool) Use(ctx context.Context, path string) error {
	c.opts.DefaultEntity = path
	err := c.DoEachConn(func(conn *Conn) error {
		return conn.Use(ctx, path)
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *ConnPool) InspectCurrentKeyspace(ctx context.Context) (*protocol.TypedArray, error) {
	return c.InspectKeyspace(ctx, "")
}

func (c *ConnPool) InspectKeyspace(ctx context.Context, name string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.InspectKeyspace(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.InspectKeyspace(ctx, name)
}

func (c *ConnPool) CreateTable(ctx context.Context, path string, modelDesc any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.CreateTable(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.CreateTable(ctx, path, modelDesc)
}

func (c *ConnPool) DropTable(ctx context.Context, path string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.DropTable(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.DropTable(ctx, path)
}

// func (c *ConnPool) UseTable(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) InspectCurrentTable(ctx context.Context) (interface{}, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) InspectTable(ctx context.Context, name string) (interface{}, error) {
// 	panic("not implemented") // TODO: Implement
// }

func (c *ConnPool) SysInfoVersion(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.SysInfoVersion(): %w", err)
	}
	defer c.pushConn(conn)
	
	return conn.SysInfoVersion(ctx)
}

func (c *ConnPool) SysInfoProtocol(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.SysInfoProtocol(): %w", err)
	}
	defer c.pushConn(conn)
	
	return conn.SysInfoProtocol(ctx)
}

func (c *ConnPool) SysInfoProtoVer(ctx context.Context) (float32, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.SysInfoProtoVer(): %w", err)
	}
	defer c.pushConn(conn)
	
	return conn.SysInfoProtoVer(ctx)
}

// func (c *ConnPool) SysMetricHealth(ctx context.Context) (string, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) SysMetricStorage(ctx context.Context) (uint64, error) {
// 	panic("not implemented") // TODO: Implement
// }
