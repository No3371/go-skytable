package skytable

import (
	"log"

	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/query"
)

func (c *Client) SetString(key string, value string) error {
	p := &QueryPacket{
		queries: []Query{
			query.NewSet(key, value),
		},
	}

	rp, err := c.ExecQuery(p)
	if err != nil {
		return err
	}

	err = rp.Err()
	if err != nil {
		return err
	}

	switch code := rp.resps[0].(type) {
	case protocol.ResponseCode:
		log.Printf("resp: %d", code)
		switch code {
		case protocol.Okay:
			return nil
		case protocol.OverwriteError:
			return &ResponseErrorCode{code}
		case protocol.ServerError:
			return &ResponseErrorCode{code}
		default:
			return ErrUnexpectedProtocol

		}
	default:
		return ErrUnexpectedProtocol
	}
}


func (c *Client) Exists(keys []string) (uint64, error) {
	p := &QueryPacket{
		queries: []Query{
			query.NewExists(keys),
		},
	}

	rp, err := c.ExecQuery(p)
	if err != nil {
		return 0, err
	}

	err = rp.Err()
	if err != nil {
		return 0, err
	}

	switch resp := rp.resps[0].(type) {
	case uint64:
		return resp, nil
	default:
		return 0, ErrUnexpectedProtocol
	}
}


func (c *Client) Del(keys []string) (uint64, error) {
	p := &QueryPacket{
		queries: []Query{
			query.NewDel(keys),
		},
	}

	rp, err := c.ExecQuery(p)
	if err != nil {
		return 0, err
	}

	err = rp.Err()
	if err != nil {
		return 0, err
	}

	switch resp := rp.resps[0].(type) {
	case uint64:
		return resp, nil
	default:
		return 0, ErrUnexpectedProtocol
	}
}