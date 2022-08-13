package skytable

import (
	"context"

	"strings"

	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
	"github.com/No3371/go-skytable/response"
)

const ProtoVer = "Skyhash-1.1"

type AuthProvider func() (username, token string, err error)
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
}

type ResponsePacket struct {
	query *QueryPacket
	resps []response.ResponseEntry
}


func (rr ResponsePacket) Resps () []response.ResponseEntry {
	return rr.resps
}

type Action interface {
	AppendToPacket(builder *strings.Builder) error
	ValidateProtocol(response interface{}) error
}

type Skytable interface {
	Heya(ctx context.Context, echo string) error
	AuthLogin(ctx context.Context, authProvider AuthProvider) error

	Exists(ctx context.Context, keys []string) (existing uint64, err error)
	Del(ctx context.Context, keys []string) (deleted uint64, err error)

	Get(ctx context.Context, key string) (response.ResponseEntry, error)
	GetString(ctx context.Context, key string) (string, error)
	GetBytes(ctx context.Context, key string) ([]byte, error)

	MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error)
	MSet(ctx context.Context, keys []string, values []any) (set uint64, err error)
	MSetA(ctx context.Context, entries []action.MSetAEntry) (set uint64, err error)

	Set (ctx context.Context, key string, value any) error
	Update (ctx context.Context, key string, value any) error

	// Pop(ctx context.Context, key string) (response.ResponseEntry, error)

	Exec(ctx context.Context, packet *QueryPacket) ([]response.ResponseEntry, error)
	ExecSingleRawQuery(segments ...string) (response.ResponseEntry, error)
	ExecRawQuery(actions ...string) ([]response.ResponseEntry, error)

	Use(ctx context.Context, path string) error

	InspectKeyspaces(ctx context.Context) (*protocol.TypedArray, error)
	CreateKeyspace(ctx context.Context, name string) error
	DropKeyspace(ctx context.Context, name string) error
	InspectCurrentKeyspace(ctx context.Context) (*protocol.TypedArray, error)
	InspectKeyspace(ctx context.Context, name string) (*protocol.TypedArray, error)

	CreateTable(ctx context.Context, path string, modelDesc any) error
	DropTable(ctx context.Context, path string) error
	// InspectCurrentTable (ctx context.Context) (interface{}, error)
	// InspectTable (ctx context.Context, name string) (interface{}, error)

	SysInfoVersion(ctx context.Context) (string, error)
	SysInfoProtocol(ctx context.Context) (string, error)
	SysInfoProtoVer(ctx context.Context) (float32, error)
	// SysMetricHealth (ctx context.Context) (string, error)
	// SysMetricStorage (ctx context.Context) (uint64, error)
}