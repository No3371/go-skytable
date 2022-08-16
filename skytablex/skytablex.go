package skytablex

import (
	"context"
	"encoding/binary"
	
	"fmt"
	"time"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
)

type ConnX struct {
	skytable.Conn
}

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

	switch resp := resps[0].Value.(type) {
	case []byte:
		break
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespNil:
			return nil, time.Time{}, protocol.ErrCodeNil
		}
	default:
		return nil, time.Time{}, protocol.NewUnexpectedProtocolError(fmt.Sprintf("GetWithSimTTL(): Unexpected response element: %v", resp), nil)
	}
	
	switch resp := resps[1].Value.(type) {
	case []byte:
		break
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespNil:
			return nil, time.Time{}, protocol.ErrCodeNil
		}
	default:
		return nil, time.Time{}, protocol.NewUnexpectedProtocolError(fmt.Sprintf("GetWithSimTTL(): Unexpected TTL element: %v", resp), nil)
	}

	return rp.Resps()[0].Value.([]byte), time.UnixMilli(int64(binary.BigEndian.Uint64(resps[1].Value.([]byte)))), nil
}

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

// SimTTL only works with BinaryString values
func (c *ConnX) USetWithSimTTL(ctx context.Context, entries ...action.KVPair) error {
    ts := make([]byte, 8)
    binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixMilli()))

	newEntries := make([]action.KVPair, len(entries) * 2)
	for i, entry := range entries {
		newEntries[i] = entry
		newEntries[i + 1] = action.KVPair{ K: entry.K + "_timestamp", V: ts }
	}

	c.USet(ctx, newEntries...)

	return nil
}

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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("UpdateWithSimTTL(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("UpdateWithSimTTL(): Unexpected response element: %v", resp), nil)
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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("UpdateWithSimTTL(): Unexpected response code: %s", resp), nil)

		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("UpdateWithSimTTL(): Unexpected response element: %v", resp), nil)
	}

	return nil
}

// SimTTL only works with BinaryString values
func (c *ConnX) DelWithSimTTL(ctx context.Context, key string) (err error) {
	p := skytable.NewQueryPacket( []skytable.Action {
		action.NewDel( []string { key, key + "_timestamp" }),
	})

	rp, err := c.BuildAndExecQuery(p)
	if err != nil {
		return err
	}

	resps := rp.Resps()
	if resps[0].Err != nil {
		return fmt.Errorf("DelWithSimTTL(): %w", err)
	}

	if resps[0].Value != uint64(2) {
		return fmt.Errorf("DelWithSimTTL(): Expecting result (deleted): 2, but got: %d", resps[0].Value)
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

// SimTTL only works with BinaryString values
func (c *ConnPoolX) GetWithSimTTL(ctx context.Context, key string) (resp []byte, tsUnix time.Time, err error) {
	conn, pusher, err := c.RentConn(false)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("*ConnPoolX.GetWithSimTTL(): %w", err)
	}
	defer pusher ()

	x := ConnX{ *conn }
	return x.GetWithSimTTL(ctx, key)
}

// SimTTL only works with BinaryString values
func (c *ConnPoolX) SetWithSimTTL(ctx context.Context, key string, value []byte) error {
	conn, pusher, err := c.RentConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPoolX.SetWithSimTTL(): %w", err)
	}
	defer pusher ()

	x := ConnX{ *conn }
	return x.SetWithSimTTL(ctx, key, value)
}

// SimTTL only works with BinaryString values
func (c *ConnPoolX) USetWithSimTTL(ctx context.Context, entries ...action.KVPair) error {
	conn, pusher, err := c.RentConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPoolX.USetWithSimTTL(): %w", err)
	}
	defer pusher ()

	x := ConnX{ *conn }
	return x.USetWithSimTTL(ctx, entries...)
}

// SimTTL only works with BinaryString values
func (c *ConnPoolX) UpdateWithSimTTL(ctx context.Context, key string, value []byte) error {
	conn, pusher, err := c.RentConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPoolX.UpdateWithSimTTL(): %w", err)
	}
	defer pusher ()

	x := ConnX{ *conn }
	return x.UpdateWithSimTTL(ctx, key, value)
}

// SimTTL only works with BinaryString values
func (c *ConnPoolX) DelWithSimTTL(ctx context.Context, key string) (err error) {
	conn, pusher, err := c.RentConn(false)
	if err != nil {
		return fmt.Errorf("*ConnPoolX.DelWithSimTTL(): %w", err)
	}
	defer pusher ()

	x := ConnX{ *conn }
	return x.DelWithSimTTL(ctx, key)
}