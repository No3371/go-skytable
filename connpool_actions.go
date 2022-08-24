package skytable

import (
	"context"
	"fmt"

	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

// https://docs.skytable.io/actions/heya
//
// The method does not return anything but the error,
// because the value returned by Skytable will be automatically validated.
func (c *ConnPool) Heya(ctx context.Context, echo string) (err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Heya(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Heya(ctx, echo)
}

// *ConnPool.AuthLogin() will take all conns and do [Conn.AuthLogin]() on each, and overwrite the AuthProvider of the pool.
//
// Noted that if there's an error, it's possible that the iteration is not completed and the connections may be using different users.
func (c *ConnPool) AuthLogin(ctx context.Context, authProvider AuthProvider) error {
	c.opts.AuthProvider = authProvider
	err := c.DoEachConn(func(conn *Conn) error {
		return conn.AuthLogin(ctx, authProvider)
	})
	return err
}

// https://docs.skytable.io/actions/exists
func (c *ConnPool) Exists(ctx context.Context, keys []string) (existing uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.Exists(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Exists(ctx, keys)
}

// https://docs.skytable.io/actions/del
func (c *ConnPool) Del(ctx context.Context, keys []string) (deleted uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.Del(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Del(ctx, keys)
}

// https://docs.skytable.io/actions/get
func (c *ConnPool) Get(ctx context.Context, key string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.Get(): %w", err)
	}
	defer c.pushConn(conn)

	return c.Get(ctx, key)
}

// GetString() is a strict version of [Get] that only success if the value is stored as String in Skytable.
func (c *ConnPool) GetString(ctx context.Context, key string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.GetString(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.GetString(ctx, key)
}

// GetBytes() is a strict version of [Get] that only success if the value is stored as BinaryString in Skytable.
func (c *ConnPool) GetBytes(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.GetBytes(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.GetBytes(ctx, key)
}

// https://docs.skytable.io/actions/mget
func (c *ConnPool) MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MGet(ctx, keys)
}

// https://docs.skytable.io/actions/mset
func (c *ConnPool) MSet(ctx context.Context, keys []string, values []any) (set uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MSet(ctx, keys, values)
}

// https://docs.skytable.io/actions/mset
func (c *ConnPool) MSetA(ctx context.Context, entries []action.KVPair) (set uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MSetA(ctx, entries)
}

// https://docs.skytable.io/actions/set
func (c *ConnPool) Set(ctx context.Context, key string, value any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Set(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Set(ctx, key, value)
}

// https://docs.skytable.io/actions/update
func (c *ConnPool) Update(ctx context.Context, key string, value any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Update(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Update(ctx, key, value)
}

// https://docs.skytable.io/actions/uset
func (c *ConnPool) USet(ctx context.Context, entries ...action.KVPair) (set uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.USet(ctx, entries...)
}

func (c *ConnPool) Pop(ctx context.Context, key string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.Pop(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Pop(ctx, key)
}

// PopString() is a strict version of [Pop] that only success if the value is stored as String in Skytable.
func (c *ConnPool) PopString(ctx context.Context, key string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.PopString(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.PopString(ctx, key)
}

// PopBytes() is a strict version of [Pop] that only success if the value is stored as BinaryString in Skytable.
func (c *ConnPool) PopBytes(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.PopBytes(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.PopBytes(ctx, key)
}

func (c *ConnPool) Exec(ctx context.Context, packet *QueryPacket) ([]response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.Exec(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.Exec(ctx, packet)
}

func (c *ConnPool) ExecSingleActionPacketRaw(segments ...any) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.ExecSingleActionPacketRaw(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.ExecSingleActionPacketRaw(segments...)
}

// func (c *ConnPool) InspectKeyspaces(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *ConnPool) CreateKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// https://docs.skytable.io/ddl/#inspect
func (c *ConnPool) InspectKeyspaces(ctx context.Context) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.InspectKeyspaces(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.InspectKeyspaces(ctx)
}

// https://docs.skytable.io/ddl/#keyspaces
func (c *ConnPool) CreateKeyspace(ctx context.Context, name string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.CreateKeyspace(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.CreateKeyspace(ctx, name)
}

// https://docs.skytable.io/ddl/#keyspaces-1
func (c *ConnPool) DropKeyspace(ctx context.Context, name string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.Dropkeyspace(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.DropKeyspace(ctx, name)
}

// https://docs.skytable.io/ddl/#use
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

// https://docs.skytable.io/ddl/#keyspaces-2
//
// If the supplied name is "", inspect the current keyspace
func (c *ConnPool) InspectKeyspace(ctx context.Context, name string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.InspectKeyspace(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.InspectKeyspace(ctx, name)
}

// https://docs.skytable.io/ddl/#tables
func (c *ConnPool) CreateTable(ctx context.Context, path string, modelDesc any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.CreateTable(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.CreateTable(ctx, path, modelDesc)
}

// https://docs.skytable.io/ddl/#tables-1
func (c *ConnPool) DropTable(ctx context.Context, path string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.DropTable(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.DropTable(ctx, path)
}

// https://docs.skytable.io/ddl/#tables-2
//
// If path is "", inspect the current table
func (c *ConnPool) InspectTable(ctx context.Context, path string) (protocol.ModelDescription, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.InspectTable(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.InspectTable(ctx, path)
}

// https://docs.skytable.io/actions/sys#info
func (c *ConnPool) SysInfoVersion(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.SysInfoVersion(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SysInfoVersion(ctx)
}

// https://docs.skytable.io/actions/sys#info
func (c *ConnPool) SysInfoProtocol(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.SysInfoProtocol(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SysInfoProtocol(ctx)
}

// https://docs.skytable.io/actions/sys#info
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

// https://docs.skytable.io/actions/mksnap
func (c *ConnPool) MKSnap (ctx context.Context, name string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.MKSnap(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MKSnap(ctx, name)
}

// https://docs.skytable.io/actions/whereami
func (c *ConnPool) WhereAmI (ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.WhereAmI(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.WhereAmI(ctx)
}