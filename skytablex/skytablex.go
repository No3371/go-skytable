package skytablex

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

type ConnX struct {
	skytable.Conn
}

// Get the value of a key from the current table, if it exists
//
// SimTTL only works with BinaryString values
func (c *ConnX) GetWithSimTTL(ctx context.Context, key string) (resp []byte, tsUnix time.Time, err error) {
	p := skytable.NewQueryPacket( []skytable.Action {
		action.NewGet(key),
		action.NewGet(key + "_timestamp"),
	})

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return nil, time.Time{}, err
	}

	resps := rp.Resps()
	if resps[0].Err != nil {
		return nil, time.Time{}, fmt.Errorf("GetWithSimTTL: failed to get value with key '%s': %w", key, err)
	}
	if resps[1].Err != nil {
		return nil, time.Time{}, fmt.Errorf("GetWithSimTTL: failed to get simulated TTL with key '%s.timestamp': %w", key, err)
	}

	if resps[0].DataType != protocol.DataTypeBinaryString {
		return nil, time.Time{}, fmt.Errorf("GetWithSimTTL: expecting BinaryString value but got %s", resps[0].DataType)
	}
	if resps[1].DataType != protocol.DataTypeBinaryString {
		return nil, time.Time{}, fmt.Errorf("GetWithSimTTL: expecting simulated TTL to be BinaryString but got %s", resps[1].DataType)
	}

	return rp.Resps()[0].Value.([]byte), time.UnixMilli(int64(binary.BigEndian.Uint64(resps[1].Value.([]byte)))), nil
}

// Set the value of a key in the current table, if it doesn't already exist
//
// SimTTL only works with BinaryString values
func (c *ConnX) SetWithSimTTL(ctx context.Context, key string, value []byte) error {
    ts := make([]byte, 8)
    binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixMilli()))

	p := skytable.NewQueryPacket( []skytable.Action {
		action.NewSet(key, value),
		action.NewSet(key + "_timestamp", ts),
	})

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	resps := rp.Resps()
	switch resp := resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			break
		case protocol.RespOverwriteError:
			return protocol.ErrCodeOverwriteError
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SetWithSimTTL(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SetWithSimTTL(): Unexpected response element: %v", resp), nil)
	}

	switch resp := resps[1].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			break
		case protocol.RespOverwriteError:
			return protocol.ErrCodeOverwriteError
		case protocol.RespServerError:
			return protocol.ErrCodeServerError
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SetWithSimTTL(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SetWithSimTTL(): Unexpected response element: %v", resp), nil)
	}

	return nil
}

// Update the value of an existing key in the current table
//
// SimTTL only works with BinaryString values
func (c *ConnX) UpdateWithSimTTL(ctx context.Context, key string, value []byte) error {
    ts := make([]byte, 8)
    binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixMilli()))

	p := skytable.NewQueryPacket( []skytable.Action {
		action.NewUpdate(key, value),
		action.NewUpdate(key + "_timestamp", ts),
	})

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	resps := rp.Resps()
	switch resp := resps[0].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			break
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

	switch resp := resps[1].Value.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			break
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

	return nil
}

// This is just an alias of InspectKeyspaces.
func (c *ConnX) ListAllKeyspaces(ctx context.Context) (*protocol.TypedArray, error) {
	return c.InspectKeyspaces(ctx)
}

func (c *ConnX) InspectCurrentKeyspace(ctx context.Context) (*protocol.TypedArray, error) {
	return c.InspectKeyspace(ctx, "")
}


type ConnPoolX struct {
	skytable.ConnPool
}


func (c *ConnPoolX) InspectCurrentKeyspace(ctx context.Context) (*protocol.TypedArray, error) {
	return c.InspectKeyspace(ctx, "")
}