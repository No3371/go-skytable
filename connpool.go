package skytable

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"

	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

type ConnPool struct {
	available chan *Conn

	opened int64 // atomic
	cap    int64

	remote       *net.TCPAddr
	authProvider func() (username, token string)

	OnError func(err error)
}

type ConnPoolOptions struct {
	Cap          int64
	AuthProvider func() (username, token string) // Do not keep auth info in memory
}

var DefaultConnPoolOptions = ConnPoolOptions{
	Cap: int64(runtime.NumCPU()) * 2,
}

func NewConnPool(remote *net.TCPAddr, opt ConnPoolOptions) *ConnPool {
	if opt.Cap == 0 {
		opt.Cap = int64(runtime.NumCPU()) * 2
	}

	return &ConnPool{
		opened:       0,
		cap:          opt.Cap,
		available:    make(chan *Conn, opt.Cap),
		remote:       remote,
		authProvider: opt.AuthProvider,
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
		if atomic.LoadInt64(&c.opened) < c.cap {
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
	if c.authProvider != nil {
		conn, err = NewConnAuth(c.remote, c.authProvider)
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


	atomic.AddInt64(&c.opened, 1)
	return conn, nil
}

func (c *ConnPool) Heya(ctx context.Context, echo string) (err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return err
	}
	defer c.pushConn(conn)

	return conn.Heya(ctx, echo)
}

func (c *ConnPool) AuthLogin(ctx context.Context, username string, token string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return err
	}
	defer c.pushConn(conn)

	return conn.AuthLogin(ctx, username, token)
}

func (c *ConnPool) Exists(ctx context.Context, keys []string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, err
	}
	defer c.pushConn(conn)

	return conn.Exists(ctx, keys)
}

func (c *ConnPool) Del(ctx context.Context, keys []string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, err
	}
	defer c.pushConn(conn)

	return conn.Del(ctx, keys)
}

func (c *ConnPool) Get(ctx context.Context, key string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, err
	}
	defer c.pushConn(conn)

	return c.Get(ctx, key)
}

func (c *ConnPool) GetString(ctx context.Context, key string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", err
	}
	defer c.pushConn(conn)

	return conn.GetString(ctx, key)
}

func (c *ConnPool) GetBytes(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, err
	}
	defer c.pushConn(conn)

	return conn.GetBytes(ctx, key)
}

func (c *ConnPool) MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, err
	}
	defer c.pushConn(conn)

	return conn.MGet(ctx, keys)
}

func (c *ConnPool) Set(ctx context.Context, key string, value any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return err
	}
	defer c.pushConn(conn)

	return conn.Set(ctx, key, value)
}

func (c *ConnPool) Update(ctx context.Context, key string, value any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return err
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
		return nil, fmt.Errorf("get: conn pool: failed to get conn: %w", err)
	}
	defer c.pushConn(conn)

	return conn.Exec(ctx, packet)
}

func (c *ConnPool) ExecSingleRawQuery(segments ...string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("get: conn pool: failed to get conn: %w", err)
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

// func (c *ConnPool) DropKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) UseKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) InspectCurrentKeyspace(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) InspectKeyspace(ctx context.Context, name string) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) CreateTable(ctx context.Context, name string, description any) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) DropTable(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

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
		return "", fmt.Errorf("get: conn pool: failed to get conn: %w", err)
	}
	defer c.pushConn(conn)
	
	return conn.SysInfoVersion(ctx)
}

func (c *ConnPool) SysInfoProtocol(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("get: conn pool: failed to get conn: %w", err)
	}
	defer c.pushConn(conn)
	
	return conn.SysInfoProtocol(ctx)
}

// func (c *ConnPool) SysInfoProtover(ctx context.Context) (float64, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) SysMetricHealth(ctx context.Context) (string, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) SysMetricStorage(ctx context.Context) (uint64, error) {
// 	panic("not implemented") // TODO: Implement
// }
