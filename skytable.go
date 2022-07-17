package skytable

import (
	"context"
	"errors"
	"fmt"
	"log"

	"net"
	"strings"
	"time"

	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/query"
	"github.com/No3371/go-skytable/response"
)

// type SupportedTypes interface {
//     ~string|~int8|~uint8|~int32|~uint32|~float32|[]byte|[]interface{}
// }

type Conn struct {
	createdAt time.Time
	usedAt  int64 // atomic
	netConn net.Conn
}

type Client struct {
	*Conn
	ctx context.Context
	strBuilder *strings.Builder
	respReader *response.ResponseReader
}

func NewClient (ctx context.Context, remote *net.TCPAddr) (*Client, error) {
	nc, err := net.DialTCP("tcp", nil, remote)
	if err != nil {
		return nil, err
	}

	conn := Conn {
		createdAt: time.Now(),
		usedAt: time.Now().UnixMilli(),
		netConn: nc,
	}

	return &Client{
		Conn: &conn,
		ctx: ctx,
		strBuilder: &strings.Builder{},
		respReader: response.NewResponseReader(),
	}, nil
}

func (c *Client) Close () error {
	return c.Conn.netConn.Close()
}

type Keyspace struct {
}

type QueryPacket struct {
    queries []Query
}

type ResponsePacket struct {
    query *QueryPacket
	resps []interface{}
	err error
}

func (rr ResponsePacket) Err() error {
	return rr.err
}

func (c *Client) BuildQuery (p *QueryPacket) (string, error) {
	if p.queries == nil || len(p.queries) == 0 {
		return "", errors.New("Invalid packet: no query")
	}

    c.strBuilder.Reset()
	if len(p.queries) > 1 { // pipelined
		fmt.Fprintf(c.strBuilder, "*%d\n", len(p.queries))

	} else {
		c.strBuilder.WriteString("*1\n")
	}

	for _, q := range p.queries {
		q.AppendToPacket(c.strBuilder)
	}

	return c.strBuilder.String(), nil
}


type Query interface {
	AppendToPacket (*strings.Builder) error
}



func (c *Client) CreateKeyspaceContext(ctx context.Context, name string) bool {
    return false
}


func (c *Client) AuthLogin (username string, token string) error {
	p := &QueryPacket{
		queries:    []Query{
			query.NewLogin(username, token),
		},
	}

	rp, err := c.ExecQuery(p)
	if err != nil {
		return fmt.Errorf("failed to execute auth: %w", err)
	}

	err = rp.Err()
	if err != nil {
		return fmt.Errorf("error in response: %w", err)
	}

	switch code := rp.resps[0].(type) {
	case protocol.ResponseCode:
		switch code {
		case protocol.Okay:
			return nil
		case protocol.BadCredentials:
			return &ResponseErrorCode{ code }
		default:
			return ErrUnexpectedProtocol
		}
	default:
		return ErrUnexpectedProtocol
	}
}


func (c *Client) GetString (key string) (string, error) {
	p := &QueryPacket{
		queries:    []Query{
			query.NewGet(key),
		},
	}

	rp, err := c.ExecQuery(p)
	if err != nil {
		return "", err
	}

	err = rp.Err()
	if err != nil {
		return "", err
	}

	switch resp := rp.resps[0].(type) {
	case string:
		return resp, nil
	case []byte:
		return string(resp), nil
	default:
		return "", ErrUnexpectedProtocol
	}
}

func (c *Client) ExecQuery (p *QueryPacket) (*ResponsePacket, error) {
	str, err := c.BuildQuery(p)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

    log.Printf("    writing: %s", str)
    log.Printf("    writing: %v", []byte(str))

	_, err = c.netConn.Write([]byte(str))
	if err != nil {
		return nil, fmt.Errorf("failed to write to conn: %w", err)
	}

	resps, err := c.respReader.Read(c.netConn)
	log.Printf("    resps: %v", resps)
	if err != nil {
		return &ResponsePacket{
			query: p,
			err: err,
		}, fmt.Errorf("failed to read from conn: %w", err)
	}

	return &ResponsePacket{
		query: p,
		resps: resps,
		err: err,
	}, nil
}