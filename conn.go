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
	openedAt time.Time
	usedAt    time.Time
	netConn   net.Conn

	strBuilder *strings.Builder
	respReader *response.ResponseReader

	autoReconnect bool

	closed chan struct{}
	err    error
}

func (c Conn) OpenedAt () time.Time {
	return c.openedAt
}

func (c Conn) UsedAt () time.Time {
	return c.usedAt
}

func (c *Conn) Close() {
	close(c.closed)
	c.netConn.Close()
}

func (c *Conn) errClose(err error) {
	c.err = err
	c.Close()
}

// A Conn may closes itself when errors occured when reading/writng packets
// While all the errors are being returned and can be handled, enabling auto reconnection will save you the trouble dealing with disconnection
//
// ⚠️ This could make you unaware of issues.
func (c *Conn) EnableAutoReconnect() {
	c.autoReconnect = true
}

// Err() return an error if the conn is closed due to an error
func (c *Conn) Err() error {
	return c.err
}

func (c *Conn) reconnect () error {
	nc, err := net.DialTCP("tcp", nil, c.netConn.RemoteAddr().(*net.TCPAddr))
	if err != nil {
		return err
	}

	c.strBuilder.Reset()
	c.closed = make(chan struct{})
	c.openedAt = time.Now()
	c.usedAt = time.Now()
	c.err = nil
	c.netConn = nc

	pv, err := c.SysInfoProtocol(context.Background())
	if err != nil {
		return fmt.Errorf("conn: failed to get protocol version: %w", err)
	}

	if pv != ProtoVer {
		return protocol.ErrProtocolVersion
	}

	return nil
}

func (c *Conn) checkClosed () error {
	select {
	case <-c.closed:
		if c.autoReconnect {
			err := c.reconnect()
			if err != nil {
				return fmt.Errorf("failed to reconnect: %w (previous: %s)", err, c.err)
			}
			return nil
		} else {
			return NewUsageError("the conn is already closed.", c.err)
		}
	default:
		return nil
	}
}

// Create a new Conn.
// If auth is enabled on the destination server, use [NewConnAuth] instead.
//
// After connection established, the driver automatically validate Skyhash protocol version with the server,
// and return an error in case of mismatch.
func NewConn(remote *net.TCPAddr) (*Conn, error) {

	nc, err := net.DialTCP("tcp", nil, remote)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		openedAt: time.Now(),
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

// Create a new Conn and ``AUTH LOGIN'' with the provided auth info.
//
// After connection established, the driver automatically
// validate Skyhash protocol version with the server,
// and return an error in case of mismatch.
func NewConnAuth(remote *net.TCPAddr, authProvider AuthProvider) (*Conn, error) {

	nc, err := net.DialTCP("tcp", nil, remote)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		openedAt: time.Now(),
		usedAt:    time.Now(),
		netConn:   nc,

		strBuilder: &strings.Builder{},
		respReader: response.NewResponseReader(),
		closed:     make(chan struct{}),
	}

	if authProvider != nil {
		err = conn.AuthLogin(context.Background(), authProvider)
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

func (c *Conn) BuildSingleActionPacketRaw(segs []string) (raw string, err error) {
	c.strBuilder.Reset()
	_, err = fmt.Fprint(c.strBuilder, "*1\n")
	if err != nil {
		return "", err
	}

	err = c.appendSingleActionRaw(segs)
	if err != nil {
		return "", err
	}

	return c.strBuilder.String(), nil
}

func (c *Conn) appendSingleActionRaw(segs []string) (err error) {
	_, err = fmt.Fprintf(c.strBuilder, "~%d\n", len(segs))
	if err != nil {
		return err
	}

	for _, s := range segs {
		_, err = fmt.Fprintf(c.strBuilder, "%d\n%s\n", len(s), s)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Conn) ExecRaw(query string) (*RawResponsePacket, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}

	_, err := c.netConn.Write([]byte(query))
	if err != nil {
		c.errClose(err)
		return nil, NewComuError("failed to write to conn", err)
	}

	resps, err := c.respReader.Read(c.netConn)
	if err != nil {
		c.errClose(err)
		return nil, NewComuError("failed to read from conn", err)
	}

	c.usedAt = time.Now()

	return &RawResponsePacket{
		resps: resps,
	}, nil
}

type BuiltQuery struct {
	*QueryPacket
	string
}

func (c *Conn) ExecQuery(bq BuiltQuery) (*ResponsePacket, error) {
	select {
	default:
	case <-bq.ctx.Done():
		return nil, bq.ctx.Err()
	}

	if err := c.checkClosed(); err != nil {
		return nil, err
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

	for i := 0; i < len(resps); i++ {
		if protoErr := bq.actions[i].ValidateProtocol(resps[i].Value); protoErr != nil {
			resps[i].Err = protoErr
		}
	}

	c.usedAt = time.Now()

	return &ResponsePacket{
		query: bq.QueryPacket,
		resps: resps,
	}, nil
}

func (c *Conn) BuildQuery(p *QueryPacket) (BuiltQuery, error) {
	select {
	default:
	case <-p.ctx.Done():
		return BuiltQuery{}, p.ctx.Err()
	}

	if err := c.checkClosed(); err != nil {
		return BuiltQuery{}, err
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
		err := q.AppendToPacket(c.strBuilder)
		if err != nil {
			return BuiltQuery{}, err
		}
	}

	c.usedAt = time.Now()

	return BuiltQuery{p, c.strBuilder.String()}, nil
}

func (c *Conn) BuildAndExecQuery(p *QueryPacket) (*ResponsePacket, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}

	bq, err := c.BuildQuery(p)
	if err != nil {
		return nil, fmt.Errorf("failed building: %w", err)
	}

	rp, err := c.ExecQuery(bq)
	if err != nil {
		return nil, fmt.Errorf("failed execution: %w", err)
	}

	c.usedAt = time.Now()

	return rp, nil
}
