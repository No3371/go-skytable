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
func (c *Conn) Heya(ctx context.Context, echo string) error {

	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Heya{ Echo: echo },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	return nil
}

// https://docs.skytable.io/actions/auth#login
func (c *Conn) AuthLogin(ctx context.Context, authProvider AuthProvider) error {
	username, token, err := authProvider()
	if err != nil {
		return fmt.Errorf("*ConnNew.AuthLogin(): auth provider returned an error: %w", err)
	}

	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Login{ Username: username, Token: token },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch code := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch code {
		case protocol.RespOkay:
			return nil
		case protocol.RespBadCredentials:
			return protocol.ErrCodeBadCredentials
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGIN: Unexpected response code: %s", code), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Unexpected response element: %v", code), nil)
	}
}

// https://docs.skytable.io/actions/exists
func (c *Conn) Exists(ctx context.Context, keys []string) (existing uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Exists{ Keys: keys },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("Exists(): Unexpected response code: %s", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("Exists(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/del
func (c *Conn) Del(ctx context.Context, keys []string) (deleted uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Del { Keys: keys },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return 0, protocol.ErrCodeServerError
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("Del(): Unexpected response code: %s", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("Del(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/sdel
func (c *Conn) SDel(ctx context.Context, keys []string) (err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.SDel { Keys: keys },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return protocol.ErrCodeNil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SDel(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SDel(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/get
func (c *Conn) Get(ctx context.Context, key string) (response.ResponseEntry, error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Get { Key: key },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	return rp.resps[0], nil
}

// GetString() is a strict version of [Get] that only success if the value is stored as String in Skytable.
func (c *Conn) GetString(ctx context.Context, key string) (string, error) {
	rp, err := c.Get(ctx, key)
	if err != nil {
		return "", err
	}

	switch resp := rp.Value.(type) {
	case string:
		return resp, nil
	case []byte:
		return string(resp), protocol.ErrWrongDataType
	default:
		return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("GetString(): Unexpected response element: %v", resp), nil)
	}
}

// GetBytes() is a strict version of [Get] that only success if the value is stored as BinaryString in Skytable.
func (c *Conn) GetBytes(ctx context.Context, key string) ([]byte, error) {
	rp, err := c.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	switch resp := rp.Value.(type) {
	case string:
		return []byte(resp), protocol.ErrWrongDataType
	case []byte:
		return resp, nil
	default:
		return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("GetBytes(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/mget
func (c *Conn) MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.MGet{ Keys: keys },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return nil, err
	}

	if rp.resps[0].Err != nil {
		return nil, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case *protocol.TypedArray:
		return resp, nil
	default:
		return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MGet(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/mset
func (c *Conn) MSetB(ctx context.Context, keys []string, values []any) (set uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.MSetB{ Keys: keys, Values: values },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return 0, protocol.ErrCodeServerError
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSetB(): Unexpected response code: %s", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSetB(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/mset
//
// This is just an alternative MSet with different signature.
func (c *Conn) MSet(ctx context.Context, entries []action.KVPair) (set uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.MSetA{ Entries: entries },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return 0, protocol.ErrCodeServerError
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSet(): Unexpected response code: %s", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSet(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/set
func (c *Conn) Set(ctx context.Context, key string, value any) error {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Set{ Key: key, Value: value },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespOverwriteError:
			return protocol.ErrCodeOverwriteError
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Set(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Set(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/update
func (c *Conn) Update(ctx context.Context, key string, value any) error {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Update{ Key: key, Value: value },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return protocol.ErrCodeNil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Update(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Update(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/mupdate
func (c *Conn) MUpdate(ctx context.Context, entries []action.KVPair) (updated uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.MUpdate{ Entries: entries },
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return 0, protocol.ErrCodeServerError
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MUpdate(): Unexpected response code: %s", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("MUpdate(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/uset
func (c *Conn) USet(ctx context.Context, entries ...action.KVPair) (set uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.USet{Entries: entries},
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return 0, protocol.ErrCodeServerError
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("USet(): Unexpected response code: %s", resp), nil)

		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("USet(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/pop
func (c *Conn) Pop(ctx context.Context, key string) (response.ResponseEntry, error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.Pop{Key: key},
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	return rp.resps[0], nil
}

// PopString() is a strict version of [Pop] that only success if the value is stored as String in Skytable.
func (c *Conn) PopString(ctx context.Context, key string) (string, error) {
	rp, err := c.Pop(ctx, key)
	if err != nil {
		return "", err
	}

	switch resp := rp.Value.(type) {
	case string:
		return resp, nil
	case []byte:
		return string(resp), protocol.ErrWrongDataType
	default:
		return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("PopString(): Unexpected response element: %v", resp), nil)
	}
}

// PopBytes() is a strict version of [Pop] that only success if the value is stored as BinaryString in Skytable.
func (c *Conn) PopBytes(ctx context.Context, key string) ([]byte, error) {
	rp, err := c.Pop(ctx, key)
	if err != nil {
		return nil, err
	}

	switch resp := rp.Value.(type) {
	case string:
		return []byte(resp), protocol.ErrWrongDataType
	case []byte:
		return resp, nil
	default:
		return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("PopBytes(): Unexpected response element: %v", resp), nil)
	}
}

func (c *Conn) Exec(ctx context.Context, packet *QueryPacket) ([]response.ResponseEntry, error) {
	packet.ctx = ctx

	rp, err := c.BuildAndExecQuery(packet)
	if err != nil {
		return nil, err
	}

	return rp.resps, nil
}

// Allows executing a packet easily like:
//
//	c.ExecSingleActionPacketRaw("SET", "X", 100)
//
// The arguments accept any type. The arguments are formatted internally with %v so most basic types should be supported.
func (c *Conn) ExecSingleActionPacketRaw(segments ...any) (response.ResponseEntry, error) {
	raw, err := c.BuildSingleActionPacketRaw(segments)
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	rr, err := c.ExecRaw(raw)
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	return rr.resps[0], nil
}

// https://docs.skytable.io/ddl/#inspect
func (c *Conn) InspectKeyspaces(ctx context.Context) (*protocol.TypedArray, error) {
	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.InspectKeyspaces{}}))
	if err != nil {
		return nil, err
	}

	if rp.resps[0].Err != nil {
		return nil, rp.resps[0].Err
	}

	return rp.resps[0].Value.(*protocol.TypedArray), nil
}

// https://docs.skytable.io/ddl/#keyspaces
func (c *Conn) CreateKeyspace(ctx context.Context, name string) error {
	cmd := action.FormatSingleCreateKeyspacePacket(name)
	rp, err := c.ExecRaw(cmd)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateKeyspace(): Unexpected response code: %s", resp), nil)
		}
	}

	return nil
}

// https://docs.skytable.io/ddl/#keyspaces-1
func (c *Conn) DropKeyspace(ctx context.Context, name string) error {
	cmd := action.FormatSingleDropKeyspacePacket(name)

	rp, err := c.ExecRaw(cmd)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropKeyspace(): Unexpected response code: %s", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropKeyspace(): Unexpected response element: %v", resp), nil)
	}

}

// https://docs.skytable.io/ddl/#use
//
// “USE KEYSPACE” and “USE TABLE” are unified into “USE”.
func (c *Conn) Use(ctx context.Context, path string) error {
	cmd := action.FormatSingleUsePacket(path)
	rp, err := c.ExecRaw(cmd)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Update(): Unexpected response code: %s", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropKeyspace(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/ddl/#keyspaces-2
//
// If the supplied name is "", inspect the current keyspace
func (c *Conn) InspectKeyspace(ctx context.Context, name string) (*protocol.TypedArray, error) {
	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.InspectKeyspace{Name: name}}))
	if err != nil {
		return nil, err
	}

	if rp.resps[0].Err != nil {
		return nil, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case *protocol.TypedArray:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return nil, protocol.ErrCodeServerError
		default:
			return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response code: %v", resp), nil)
		}
	default:
		return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/ddl/#tables
func (c *Conn) CreateTable(ctx context.Context, path string, modelDesc any) error {
	cmd, err := action.FormatSingleCreateTablePacket(path, modelDesc)
	if err != nil {
		return err
	}

	rp, err := c.ExecRaw(cmd)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateTable(): Unexpected response code: %v", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/ddl/#tables-1
func (c *Conn) DropTable(ctx context.Context, path string) error {
	cmd := action.FormatSingleDropTablePacket(path)

	rp, err := c.ExecRaw(cmd)
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropTable(): Unexpected response code: %s", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response element: %v", resp), nil)
	}
}

// func (c *Conn) InspectCurrentTable(ctx context.Context) (interface{}, error) {
// 	panic("not implemented") // TODO: Implement
// }

// https://docs.skytable.io/ddl/#tables-2
//
// If path is "", inspect the current table
func (c *Conn) InspectTable(ctx context.Context, path string) (protocol.ModelDescription, error) {
	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.InspectTable{Path: path}}))
	if err != nil {
		return nil, err
	}

	if rp.resps[0].Err != nil {
		return nil, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case string:
		return protocol.ParseDescription(resp)
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return nil, protocol.ErrCodeServerError
		default:
			return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response code: %v", resp), nil)
		}
	default:
		return nil, protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/sys#info
func (c *Conn) SysInfoVersion(ctx context.Context) (string, error) {
	rp, err := c.ExecRaw("*1\n~3\n3\nSYS\n4\nINFO\n7\nVERSION\n")
	if err != nil {
		return "", err
	}

	if rp.resps[0].DataType != protocol.DataTypeString {
		return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysInfoVersion(): response is not string: %s", rp.resps[0].DataType.String()), nil)
	}

	return rp.resps[0].Value.(string), nil
}

// https://docs.skytable.io/actions/sys#info
func (c *Conn) SysInfoProtocol(ctx context.Context) (string, error) {
	rp, err := c.ExecRaw("*1\n~3\n3\nSYS\n4\nINFO\n8\nPROTOCOL\n")
	if err != nil {
		return "", err
	}

	if rp.resps[0].Err != nil {
		return "", rp.resps[0].Err
	}

	if rp.resps[0].DataType != protocol.DataTypeString {
		return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysInfoProtocol(): response is not string: %s %v", rp.resps[0].DataType.String(), rp.resps[0].Value), nil)
	}

	return rp.resps[0].Value.(string), nil
}

// https://docs.skytable.io/actions/sys#info
func (c *Conn) SysInfoProtoVer(ctx context.Context) (float32, error) {
	rp, err := c.ExecRaw("*1\n~3\n3\nSYS\n4\nINFO\n8\nPROTOVER\n")
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	if rp.resps[0].DataType != protocol.DataTypeFloat {
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysInfoProtoVer(): response is not a float: %s %v", rp.resps[0].DataType.String(), rp.resps[0].Value), nil)
	}

	return rp.resps[0].Value.(float32), nil
}

// func (c *Conn) SysMetricHealth(ctx context.Context) (string, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) SysMetricStorage(ctx context.Context) (uint64, error) {
// 	panic("not implemented") // TODO: Implement
// }

// https://docs.skytable.io/actions/mksnap
//
// If name is "", it will only send "MKSNAP"
func (c *Conn) MKSnap(ctx context.Context, name string) error {
	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.MKSnap{Name: name}}))
	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response code: %v", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/whereami
func (c *Conn) WhereAmI (ctx context.Context) (string, error) {
	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.WhereAmI{}}))
	if err != nil {
		return "", err
	}

	if rp.resps[0].Err != nil {
		return "", rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case *protocol.TypedArray:
		if len(resp.Elements) == 1 {
			return resp.Elements[0].(string), nil
		} else {
			return fmt.Sprintf("%s:%s", resp.Elements[0].(string), resp.Elements[1].(string)), nil
		}
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return "", protocol.ErrCodeServerError
		default:
			return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI(): Unexpected response code: %v", resp), nil)
		}
	default:
		return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/dbsize
func (c *Conn) DBSize (ctx context.Context, entity string) (size uint64, err error) {
	var rp *RawResponsePacket
	if entity == "" {
		rp, err = c.ExecRaw("*1\n~1\n6\nDBSIZE\n")
	} else {
		rp, err = c.ExecRaw(fmt.Sprintf("*1\n~2\n6\nDBSIZE\n%d\n%s\n", len(entity), entity))
	}

	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return 0, protocol.ErrCodeServerError
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("DBSize(): Unexpected response code: %v", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("DBSize(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/keylen
func (c *Conn) KeyLen (ctx context.Context, key string) (uint64, error) {
	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.KeyLen{}}))
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespNil:
			return 0, protocol.ErrCodeNil
		default:
			return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("KeyLen(): Unexpected response code: %v", resp), nil)
		}
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("KeyLen(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/sys#metric
func (c *Conn) SysMetricHealth (ctx context.Context) (bool, error) {
	rp, err := c.ExecRaw("*1\n~3\n3\nSYS\n6\nMETRIC\n6\nHEALTH\n")
	if err != nil {
		return false, err
	}

	if rp.resps[0].Err != nil {
		return false, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case string:
		switch resp {
		case "good":
			return true, nil
		case "critical":
			return false, nil
		default:
			return false, protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysMetricHealth(): Unexpected response string: %v", resp), nil)
		}
	default:
		return false, protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysMetricHealth(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/sys#metric
func (c *Conn) SysMetricStorage (ctx context.Context) (uint64, error) {
	rp, err := c.ExecRaw("*1\n~3\n3\nSYS\n6\nMETRIC\n7\nSTORAGE\n")
	if err != nil {
		return 0, err
	}

	if rp.resps[0].Err != nil {
		return 0, rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case uint64:
		return resp, nil
	default:
		return 0, protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysMetricStorage(): Unexpected response element: %v", resp), nil)
	}
}

// https://docs.skytable.io/actions/flushdb
//
// If entity is "", flush the current table
func (c *Conn) FlushDB (ctx context.Context, entity string) (err error) {
	var rp *RawResponsePacket
	if entity == "" {
		rp, err = c.ExecRaw("*1\n~1\n7\nFLUSHDB\n")
	} else {
		rp, err = c.ExecRaw(fmt.Sprintf("*1\n~2\n7\nFLUSHDB\n%d\n%s\n", len(entity), entity))
	}

	if err != nil {
		return err
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	switch resp := rp.resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("FlushDB(): Unexpected response code: %v", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("FlushDB(): Unexpected response element: %v", resp), nil)
	}
}