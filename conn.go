package skytable

import (
	"context"
	"fmt"

	"net"
	"strings"
	"time"

	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

type Conn struct {
	createdAt time.Time
	usedAt    time.Time
	netConn   net.Conn

	strBuilder *strings.Builder
	respReader *response.ResponseReader

	closed chan struct{}
	err    error
}


func (c *Conn) Close() {
	close(c.closed)
}

func (c *Conn) errClose(err error) {
	c.err = err
	close(c.closed)
}

// Err() return an error if the conn is closed due to an error
func (c *Conn) Err() error {
	return c.err
}

func NewConn(remote *net.TCPAddr) (*Conn, error) {

	nc, err := net.DialTCP("tcp", nil, remote)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		createdAt: time.Now(),
		usedAt:    time.Now(),
		netConn:   nc,

		strBuilder: &strings.Builder{},
		respReader: response.NewResponseReader(),
		closed:     make(chan struct{}),
	}

	pv, err := conn.SysInfoProtocol(context.Background())
	if err != nil {
		return nil, fmt.Errorf("conn: failed to get protocol version: %w", err)
	}

	if pv != ProtoVer {
		return nil, protocol.ErrProtocolVersion
	}

	return conn, nil
}

func NewConnAuth(remote *net.TCPAddr, authProvider func() (username, token string)) (*Conn, error) {

	nc, err := net.DialTCP("tcp", nil, remote)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		createdAt: time.Now(),
		usedAt:    time.Now(),
		netConn:   nc,

		strBuilder: &strings.Builder{},
		respReader: response.NewResponseReader(),
		closed:     make(chan struct{}),
	}

	if authProvider != nil {
		u, t := authProvider()
		err = conn.AuthLogin(context.Background(), u, t)
		if err != nil {
			return nil, fmt.Errorf("conn pool: conn: failed to auth login: %w", err)
		}
	}

	pv, err := conn.SysInfoProtocol(context.Background())
	if err != nil {
		return nil, fmt.Errorf("conn: failed to get protocol version: %w", err)
	}

	if pv != ProtoVer {
		return nil, protocol.ErrProtocolVersion
	}

	return conn, nil
}

func (c *Conn) BuildSingleRaw(segs ...string) (raw string, err error) {
	c.strBuilder.Reset()
	for i, s := range segs {
		if i != 0 {
			_, err = c.strBuilder.WriteRune(' ')
			if err != nil {
				return "", err
			}
		}
		_, err = c.strBuilder.WriteString(s)
		if err != nil {
			return "", err
		}
	}

	return c.strBuilder.String(), nil
}

func (c *Conn) ExecRaw(query []byte) (*RawResponsePacket, error) {
	select {
	case <-c.closed:
		return nil, NewUsageError("the conn is already closed: ", c.err)
	default:
	}

	_, err := c.netConn.Write(query)
	if err != nil {
		c.errClose(err)
		return nil, NewComuError("failed to write to conn", err)
	}

	resps, err := c.respReader.Read(c.netConn)
	if err != nil {
		c.errClose(err)
		return nil, NewComuError("failed to read from conn", err)
	}

	return &RawResponsePacket{
		resps: resps,
		err:   err,
	}, nil
}

type BuiltQuery struct {
	*QueryPacket
	string
}

func (c *Conn) ExecQuery(bq BuiltQuery) (*ResponsePacket, error) {
	select {
	case <-c.closed:
		return nil, NewUsageError("the conn is already closed: ", c.err)
	default:
	}

	_, err := c.netConn.Write([]byte(bq.string))
	if err != nil {
		c.errClose(err)
		return nil, NewComuError("failed to write to conn", err)
	}

	resps, err := c.respReader.Read(c.netConn)
	if err != nil {
		c.errClose(err)
		return nil, NewComuError("failed to read from conn", err)
	}

	if len(bq.actions) != len(resps) {
		panic("response entry count mismatch")
	}

	for i := 0; i < len(bq.actions); i++ {
		if protoErr := bq.actions[i].ValidateProtocol(resps[i].Value); protoErr != nil {
			resps[i].Err = protoErr
		}
	}

	return &ResponsePacket{
		query: bq.QueryPacket,
		resps: resps,
	}, nil
}

func (c *Conn) BuildQuery(p *QueryPacket) (BuiltQuery, error) {
	select {
	case <-c.closed:
		return BuiltQuery{}, NewUsageError("the conn is already closed: ", c.err)
	default:
	}

	if p.actions == nil || len(p.actions) == 0 {
		return BuiltQuery{p, ""}, NewUsageError("empty packet (0 action)", nil)
	}

	c.strBuilder.Reset()
	if len(p.actions) > 1 { // pipelined
		fmt.Fprintf(c.strBuilder, "*%d\n", len(p.actions))
	} else {
		c.strBuilder.WriteString("*1\n")
	}

	for _, q := range p.actions {
		q.AppendToPacket(c.strBuilder)
	}

	return BuiltQuery{p, c.strBuilder.String()}, nil
}

func (c *Conn) BuildAndExecQuery(p *QueryPacket) (*ResponsePacket, error) {
	select {
	case <-c.closed:
		return nil, NewUsageError("the conn is already closed: ", c.err)
	default:
	}

	bq, err := c.BuildQuery(p)
	if err != nil {
		return nil, fmt.Errorf("failed building: %w", err)
	}

	rp, err := c.ExecQuery(bq)
	if err != nil {
		return nil, fmt.Errorf("failed execution: %w", err)
	}

	return rp, nil
}
