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

// *ConnPool.AuthLogin() will take all conns and do [Conn.AuthLogout]() on each, and overwrite the AuthProvider of the pool.
//
// Noted that if there's an error, it's possible that the iteration is not completed and the connections may be using different users.
func (c *ConnPool) AuthLogout(ctx context.Context) error {
	err := c.DoEachConn(func(conn *Conn) error {
		return conn.AuthLogout(ctx)
	})
	return err
}

// https://docs.skytable.io/actions/auth#claim
func (c *ConnPool) AuthClaim(ctx context.Context, originKey string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.AuthClaim(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthClaim(ctx, originKey)
}

// https://docs.skytable.io/actions/auth#adduser
func (c *ConnPool) AuthAddUser(ctx context.Context, username string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.AuthAddUser(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthAddUser(ctx, username)
}

// https://docs.skytable.io/actions/auth#deluser
func (c *ConnPool) AuthDelUser(ctx context.Context, username string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.AuthDelUser(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthDelUser(ctx, username)
}

// https://docs.skytable.io/actions/auth#restore
//
// If provided `originKey` is "", it'll be omitted in the sent command
func (c *ConnPool) AuthRestore(ctx context.Context, originKey string, username string) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.AuthRestore(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthRestore(ctx, originKey, username)
}

// https://docs.skytable.io/actions/auth#listuser
func (c *ConnPool) AuthListUser(ctx context.Context) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.AuthListUser(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthListUser(ctx)
}

// https://docs.skytable.io/actions/auth#whoami
func (c *ConnPool) AuthWhoAmI(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.AuthWhoAmI(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.AuthWhoAmI(ctx)
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

// https://docs.skytable.io/actions/sdel
func (c *ConnPool) SDel(ctx context.Context, keys []string) (err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.SDel(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SDel(ctx, keys)
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
func (c *ConnPool) MSetB(ctx context.Context, keys []string, values []any) (set uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MSetB(ctx, keys, values)
}

// https://docs.skytable.io/actions/mset
func (c *ConnPool) MSet(ctx context.Context, entries []action.KVPair) (set uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.MGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MSet(ctx, entries)
}

// https://docs.skytable.io/actions/sset
func (c *ConnPool) SSet(ctx context.Context, entries []action.KVPair) (err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.SGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SSet(ctx, entries)
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

// https://docs.skytable.io/actions/update
func (c *ConnPool) MUpdate(ctx context.Context, entries []action.KVPair) (updated uint64, err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.MUpdate(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MUpdate(ctx, entries)
}

// https://docs.skytable.io/actions/supdate
func (c *ConnPool) SUpdate(ctx context.Context, entries []action.KVPair) (err error) {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.SUpdate(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SUpdate(ctx, entries)
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

// https://docs.skytable.io/actions/mpop
func (c *ConnPool) MPop(ctx context.Context, keys []string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.MPop(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MPop(ctx, keys)
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
//
// If name is "", it will only send "MKSNAP"
func (c *ConnPool) MKSnap(ctx context.Context, name string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.MKSnap(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.MKSnap(ctx, name)
}

// https://docs.skytable.io/actions/whereami
func (c *ConnPool) WhereAmI(ctx context.Context) (string, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return "", fmt.Errorf("*ConnPool.WhereAmI(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.WhereAmI(ctx)
}

// https://docs.skytable.io/actions/dbsize
func (c *ConnPool) DBSize(ctx context.Context, entity string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.DBSize(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.DBSize(ctx, entity)
}

// https://docs.skytable.io/actions/dbsize
func (c *ConnPool) KeyLen(ctx context.Context, key string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.KeyLen(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.KeyLen(ctx, key)
}

// https://docs.skytable.io/actions/sys#metric
//
// Returns true if "good", false when "critical"
func (c *ConnPool) SysMetricHealth(ctx context.Context) (bool, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return false, fmt.Errorf("*ConnPool.SysMetricHealth(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SysMetricHealth(ctx)
}

// https://docs.skytable.io/actions/sys#metric
func (c *ConnPool) SysMetricStorage(ctx context.Context) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.SysMetricStorage(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.SysMetricStorage(ctx)
}

// https://docs.skytable.io/actions/flushdb
//
// If entity is "", flush the current table
func (c *ConnPool) FlushDB(ctx context.Context, entity string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.FlushDB(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.FlushDB(ctx, entity)
}

// https://docs.skytable.io/actions/lget#lget
func (c *ConnPool) LGet(ctx context.Context, listName string) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.LGet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGet(ctx, listName)
}

// https://docs.skytable.io/actions/lget#limit
func (c *ConnPool) LGetLimit(ctx context.Context, listName string, limit uint64) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.LGetLimit(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGetLimit(ctx, listName, limit)
}

// https://docs.skytable.io/actions/lget#len
func (c *ConnPool) LGetLen(ctx context.Context, listName string) (uint64, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return 0, fmt.Errorf("*ConnPool.LGetLen(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGetLen(ctx, listName)
}

// https://docs.skytable.io/actions/lget#valueat
func (c *ConnPool) LGetValueAt(ctx context.Context, listName string, index uint64) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.LGetValueAt(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGetValueAt(ctx, listName, index)
}

// https://docs.skytable.io/actions/lget#first
func (c *ConnPool) LGetFirst(ctx context.Context, listName string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.LGetFirst(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGetFirst(ctx, listName)
}

// https://docs.skytable.io/actions/lget#last
func (c *ConnPool) LGetLast(ctx context.Context, listName string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.LGetLast(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGetLast(ctx, listName)
}

// https://docs.skytable.io/actions/lget#range
//
// If provided `to` is 0, it's omitted in the sent command.
func (c *ConnPool) LGetRange(ctx context.Context, listName string, from uint64, to uint64) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.LGetRange(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LGetRange(ctx, listName, from, to)
}

// https://docs.skytable.io/actions/lmod#push
func (c *ConnPool) LModPush(ctx context.Context, listName string, elements []any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.LModPush(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LModPush(ctx, listName, elements)
}

// https://docs.skytable.io/actions/lmod#insert
func (c *ConnPool) LModInsert(ctx context.Context, listName string, index uint64, element any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.LModInsert(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LModInsert(ctx, listName, index, element)
}

// https://docs.skytable.io/actions/lmod#pop
func (c *ConnPool) LModPop(ctx context.Context, listName string) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.LModPop(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LModPop(ctx, listName)
}

// https://docs.skytable.io/actions/lmod#pop
func (c *ConnPool) LModPopIndex(ctx context.Context, listName string, index uint64) (response.ResponseEntry, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return response.EmptyResponseEntry, fmt.Errorf("*ConnPool.LModPopIndex(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LModPopIndex(ctx, listName, index)
}

// https://docs.skytable.io/actions/lmod#remove
func (c *ConnPool) LModRemove(ctx context.Context, listName string, index uint64) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.LModRemove(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LModRemove(ctx, listName, index)
}

// https://docs.skytable.io/actions/lmod#clear
func (c *ConnPool) LModClear(ctx context.Context, listName string) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.LModClear(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LModClear(ctx, listName)
}

// https://docs.skytable.io/actions/lset
//
// If `elements` is nil, it's omitted in the sent command.`
func (c *ConnPool) LSet(ctx context.Context, listName string, elements []any) error {
	conn, err := c.popConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPool.LSet(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LSet(ctx, listName, elements)
}


// https://docs.skytable.io/actions/lskeys
func (c *ConnPool) LSKeys(ctx context.Context, entity string, limit uint64) (*protocol.TypedArray, error) {
	conn, err := c.popConn(false)
	if err != nil {
		return nil, fmt.Errorf("*ConnPool.LSKeys(): %w", err)
	}
	defer c.pushConn(conn)

	return conn.LSKeys(ctx, entity, limit)
}