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

func GetWithSimTTL(c *skytable.Conn, ctx context.Context, key string) (resp any, tsUnix time.Time, err error) {
	p := skytable.NewQueryPacket( []skytable.Action {
		action.NewGet(key),
		action.NewGet(key + "_timestamp"),
	})

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return response.EmptyResponseEntry, time.Time{}, err
	}

	resps := rp.Resps()
	if resps[0].Err != nil {
		return response.EmptyResponseEntry, time.Time{}, fmt.Errorf("GetWithSimTTL: failed to get value with key '%s': %w", key, err)
	}
	if resps[1].Err != nil {
		return response.EmptyResponseEntry, time.Time{}, fmt.Errorf("GetWithSimTTL: failed to get simulated TTL with key '%s.timestamp': %w", key, err)
	}

	if resps[1].DataType != protocol.DataTypeBinaryString {
		return response.EmptyResponseEntry, time.Time{}, fmt.Errorf("GetWithSimTTL: expecting simulated TTL to be BinaryString but got %s", resps[1].DataType)
	}

	return rp.Resps()[0].Value, time.UnixMilli(int64(binary.BigEndian.Uint64(resps[1].Value.([]byte)))), nil
}

func SetWithSimTTL(c *skytable.Conn, ctx context.Context, key string, value any) error {
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

func UpdateWithSimTTL(c *skytable.Conn, ctx context.Context, key string, value any) error {
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