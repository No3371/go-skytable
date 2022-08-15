package skytable

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/No3371/go-skytable/protocol"
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
	AuthProvider func() (username, token string, err error) // Do not keep auth info in memory
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

	cp := &ConnPool{
		opened:       0,
		available:    make(chan *Conn, opts.Cap),
		remote:       remote,
		opts: opts,
	}

	return cp
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

// Get a conn and return it back.
// A ``pusher'' func is returned to push back the conn.
//
// 		conn, pusher, err := c.RentConn(false)
//		if err != nil {
// 		return err
// 		}
// 		defer pusher ()
func (c *ConnPool) RentConn (dontOpenNew bool) (conn *Conn, pusher func (), err error) {
	conn, err = c.popConn(dontOpenNew)
	if err != nil {
		return nil, nil, err
	}

	pusher = func () {
		c.pushConn(conn)
	}

	return conn, pusher, err
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
