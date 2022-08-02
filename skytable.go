package skytable

import (
	"context"

	"strings"

	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

const ProtoVer = "Skyhash-1.1"

type QueryPacket struct {
	ctx     context.Context
	actions []Action
}

func NewQueryPacket (actions []Action) *QueryPacket {
	return &QueryPacket{
		actions: actions,
	}
}

type RawResponsePacket struct {
	resps []response.ResponseEntry
	err   error
}

type ResponsePacket struct {
	query *QueryPacket
	resps []response.ResponseEntry
	err   error
}

func (rr ResponsePacket) Err() error {
	return rr.err
}

func (rr ResponsePacket) Resps () []response.ResponseEntry {
	return rr.resps
}

type Action interface {
	AppendToPacket(builder *strings.Builder) error
	ValidateProtocol(response interface{}) error
}

type STConn interface {
	Heya (ctx context.Context, echo string) error
	AuthLogin(ctx context.Context, username string, token string) error

	Exists (ctx context.Context, keys []string) (uint64, error)
	Del    (ctx context.Context, keys []string) (uint64, error)

	Get(ctx context.Context, key string) (response.ResponseEntry, error)
	GetString(ctx context.Context, key string) (string, error)
	GetBytes(ctx context.Context, key string) ([]byte, error)

	MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error)

	Set (ctx context.Context, key string, value any) error
	
	Update (ctx context.Context, key string, value any) error
	UpdateString(ctx context.Context, key string, value string) error
	UpdateBytes(ctx context.Context, key string, value []byte) error

	Pop(ctx context.Context, key string) (response.ResponseEntry, error)

	Exec (ctx context.Context, packet *QueryPacket) ([]any, error)
	ExecSingleRawQuery (segments ...string) (any, error)
	ExecRawQuery (actions ...string) (any, error)

	InspectKeyspaces (ctx context.Context) (protocol.Array, error)
	ListAllKeyspaces (ctx context.Context) (protocol.Array, error)

	// CreateKeyspace (ctx context.Context, name string) error
	// DropKeyspace (ctx context.Context, name string) error
	// UseKeyspace (ctx context.Context, name string) error
	// InspectCurrentKeyspace (ctx context.Context) (protocol.Array, error)
	// InspectKeyspace (ctx context.Context, name string) (protocol.Array, error)

	// CreateTable (ctx context.Context, name string, description any) error
	// DropTable (ctx context.Context, name string) error
	// UseTable (ctx context.Context, name string) error
	// InspectCurrentTable (ctx context.Context) (interface{}, error)
	// InspectTable (ctx context.Context, name string) (interface{}, error)

	// SysInfoVersion (ctx context.Context) (string, error)
	SysInfoProtocol (ctx context.Context) (string, error)
	// SysInfoProtover (ctx context.Context) (float64, error)
	// SysMetricHealth (ctx context.Context) (string, error)
	// SysMetricStorage (ctx context.Context) (uint64, error)
}