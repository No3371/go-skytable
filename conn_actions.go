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
			action.NewHeya(echo),
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
			action.NewLogin(username, token),
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
			action.NewExists(keys),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	return rp.resps[0].Value.(uint64), nil
}

func (c *Conn) Del(ctx context.Context, keys []string) (deleted uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewDel(keys),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	return rp.resps[0].Value.(uint64), nil
}

// https://docs.skytable.io/actions/get
func (c *Conn) Get(ctx context.Context, key string) (response.ResponseEntry, error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewGet(key),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	return rp.resps[0], nil
}

// GetString() is a strict version of Get() that only success if the value is stored as String in Skytable.
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

// GetBytes() is a strict version of GET that only success if the value is stored as BinaryString in Skytable.
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

// Get the value of 'n' keys from the current table, if they exist
func (c *Conn) MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewMGet(keys),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return nil, err
	}

	return rp.resps[0].Value.(*protocol.TypedArray), nil
}

// MSet returns the actual number of the keys set.
func (c *Conn) MSet(ctx context.Context, keys []string, values []any) (set uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewMSetB(keys, values),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	return rp.resps[0].Value.(uint64), nil
}

// MSet returns the actual number of the keys set.
// This is just an alternative MSet with different signature.
func (c *Conn) MSetA(ctx context.Context, entries []action.MSetAEntry) (set uint64, err error) {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewMSetA(entries),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return 0, err
	}

	return rp.resps[0].Value.(uint64), nil
}

// Set the value of a key in the current table, if it doesn't already exist
func (c *Conn) Set(ctx context.Context, key string, value any) error {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewSet(key, value),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
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

// Update the value of an existing key in the current table
func (c *Conn) Update(ctx context.Context, key string, value any) error {
	p := &QueryPacket{
		ctx: ctx,
		actions: []Action{
			action.NewUpdate(key, value),
		},
	}

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
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

// func (c *Conn) UpdateString(ctx context.Context, key string, value string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) UpdateBytes(ctx context.Context, key string, value []byte) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) Pop(ctx context.Context, key string) (protocol.DataType, any, error) {
// 	panic("not implemented") // TODO: Implement
// }

func (c *Conn) Exec(ctx context.Context, packet *QueryPacket) ([]response.ResponseEntry, error) {
	packet.ctx = ctx

	rp, err := c.BuildAndExecQuery(packet)
	if err != nil {
		return nil, err
	}

	return rp.resps, nil
}

// Allows executing a packet easily like:
//     c.ExecSingleActionPacketRaw("SET", "X", 100)
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
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropKeyspace(): Unexpected response code: %s", resp), nil)
		}
	}

	return nil
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
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Update(): Unexpected response code: %s", resp), nil)
		}
	}

	return nil
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

	return rp.resps[0].Value.(*protocol.TypedArray), nil
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
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateTable(): Unexpected response code: %s", resp), nil)
		}
	}

	return nil
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
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropTable(): Unexpected response code: %s", resp), nil)
		}
	}

	return nil
}

// func (c *Conn) InspectCurrentTable(ctx context.Context) (interface{}, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) InspectTable(ctx context.Context, name string) (protocol.ModelDescription, error) {
// 	rp, err := c.BuildAndExecQuery(NewQueryPacket([]Action{action.InspectTable{Name: name}}))
// 	if err != nil {
// 		return nil, err
// 	}

// 	if rp.resps[0].Err != nil {
// 		return nil, rp.resps[0].Err
// 	}

// 	return rp.resps[0].Value.(*protocol.TypedArray), nil
// }

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
