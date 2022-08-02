package skytable

import (
	"context"
	"fmt"

	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

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

	if rp.Err() != nil {
		return rp.Err()
	}

	if rp.resps[0].Err != nil {
		return rp.resps[0].Err
	}

	return nil
}

func (c *Conn) AuthLogin(ctx context.Context, username string, token string) error {
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

	if rp.Err() != nil {
		return rp.Err()
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

func (c *Conn) Exists(ctx context.Context, keys []string) (uint64, error) {
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

	if rp.Err() != nil {
		return rp.resps[0].Value.(uint64), rp.Err()
	}

	return rp.resps[0].Value.(uint64), nil
}

func (c *Conn) Del(ctx context.Context, keys []string) (uint64, error) {
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

	if rp.Err() != nil {
		return rp.resps[0].Value.(uint64), rp.Err()
	}

	return rp.resps[0].Value.(uint64), nil
}

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

	if rp.Err() != nil {
		return rp.resps[0], rp.Err()
	}

	return rp.resps[0], nil
}

// GetBytes() is a strict version of GET that only success if the value is stored as String in Skytable. 
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

	if rp.Err() != nil {
		return rp.resps[0].Value.(*protocol.TypedArray), rp.Err()
	}

	return rp.resps[0].Value.(*protocol.TypedArray), nil
}

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

	if rp.Err() != nil {
		return rp.Err()
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

	if rp.Err() != nil {
		return rp.Err()
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

	if rp.Err() != nil {
		return rp.resps, rp.Err()
	}

	return rp.resps, nil
}

func (c *Conn) ExecSingleRawQuery(segments ...string) (response.ResponseEntry, error) {
	raw, err := c.BuildSingleRaw(segments...)
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	rr, err := c.ExecRaw([]byte(raw))
	if err != nil {
		return response.EmptyResponseEntry, err
	}

	if rr.err != nil {
		return rr.resps[0], err
	}

	return rr.resps[0], nil
}

// func (c *Conn) ExecRawQuery(actions ...string) (any, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) InspectKeyspaces(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) ListAllKeyspaces(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) CreateKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) DropKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) UseKeyspace(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) InspectCurrentKeyspace(ctx context.Context) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) InspectKeyspace(ctx context.Context, name string) (protocol.Array, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) CreateTable(ctx context.Context, name string, description any) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) DropTable(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) UseTable(ctx context.Context, name string) error {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) InspectCurrentTable(ctx context.Context) (interface{}, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) InspectTable(ctx context.Context, name string) (interface{}, error) {
// 	panic("not implemented") // TODO: Implement
// }

func (c *Conn) SysInfoVersion(ctx context.Context) (string, error) {
	rp, err := c.ExecRaw([]byte("*1\n~3\n3\nSYS\n4\nINFO\n7\nVERSION\n"))
	if err != nil {
		return "", err
	}

	if rp.err != nil {
		return "", err
	}

	if rp.resps[0].DataType != protocol.DataTypeString {
		return "", protocol.NewUnexpectedProtocolError(fmt.Sprintf("SysInfoVersion(): response is not string: %s", rp.resps[0].DataType.String()), nil)
	}

	return rp.resps[0].Value.(string), nil
}

func (c *Conn) SysInfoProtocol(ctx context.Context) (string, error) {
	rp, err := c.ExecRaw([]byte("*1\n~3\n3\nSYS\n4\nINFO\n8\nPROTOCOL\n"))
	if err != nil {
		return "", err
	}

	if rp.err != nil {
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

// func (c *Conn) SysInfoProtover(ctx context.Context) (float64, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) SysMetricHealth(ctx context.Context) (string, error) {
// 	panic("not implemented") // TODO: Implement
// }

// func (c *Conn) SysMetricStorage(ctx context.Context) (uint64, error) {
// 	panic("not implemented") // TODO: Implement
// }
