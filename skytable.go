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

type Action interface {
	AppendToPacket(builder *strings.Builder) error
	ValidateProtocol(response interface{}) error
}

type Skytable interface {
	Heya(ctx context.Context, echo string) error                    // https://docs.skytable.io/actions/heya
	AuthLogin(ctx context.Context, authProvider AuthProvider) error // https://docs.skytable.io/actions/auth#login

	Exists(ctx context.Context, keys []string) (existing uint64, err error) // https://docs.skytable.io/actions/exists
	Del(ctx context.Context, keys []string) (deleted uint64, err error)     // https://docs.skytable.io/actions/del

	Get(ctx context.Context, key string) (response.ResponseEntry, error) // https://docs.skytable.io/actions/get
	GetString(ctx context.Context, key string) (string, error)           // a strict version of [Get] that only success if the value is stored as String in Skytable.
	GetBytes(ctx context.Context, key string) ([]byte, error)            // a strict version of [Get] that only success if the value is stored as BinaryString in Skytable.

	MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error)         // https://docs.skytable.io/actions/mset
	MSet(ctx context.Context, keys []string, values []any) (set uint64, err error) // https://docs.skytable.io/actions/mset
	MSetA(ctx context.Context, entries []action.KVPair) (set uint64, err error)    // https://docs.skytable.io/actions/mset

	Set(ctx context.Context, key string, value any) error    // https://docs.skytable.io/actions/set
	Update(ctx context.Context, key string, value any) error // https://docs.skytable.io/actions/update

	USet(ctx context.Context, entries ...action.KVPair) (set uint64, err error)

	Pop(ctx context.Context, key string) (response.ResponseEntry, error)

	Exec(ctx context.Context, packet *QueryPacket) ([]response.ResponseEntry, error)
	ExecSingleActionPacketRaw(segments ...any) (response.ResponseEntry, error)

	// https://docs.skytable.io/ddl/#use
	//
	// ``USE KEYSPACE'' and ``USE TABLE'' are unified into ``USE''.
	Use(ctx context.Context, path string) error

	InspectKeyspaces(ctx context.Context) (*protocol.TypedArray, error)             // https://docs.skytable.io/ddl/#inspect
	CreateKeyspace(ctx context.Context, name string) error                          // https://docs.skytable.io/ddl/#keyspaces
	DropKeyspace(ctx context.Context, name string) error                            // https://docs.skytable.io/ddl/#keyspaces-1
	// https://docs.skytable.io/ddl/#keyspaces-2
	//
	// If name is "", inspect the current keyspace
	InspectKeyspace(ctx context.Context, name string) (*protocol.TypedArray, error)

	CreateTable(ctx context.Context, path string, modelDesc any) error // https://docs.skytable.io/ddl/#tables
	DropTable(ctx context.Context, path string) error                  // https://docs.skytable.io/ddl/#tables-1
	// https://docs.skytable.io/ddl/#tables-2
	//
	// If path is "", inspect the current table
	InspectTable (ctx context.Context, path string) (protocol.ModelDescription, error)

	SysInfoVersion(ctx context.Context) (string, error)   // https://docs.skytable.io/actions/sys#info
	SysInfoProtocol(ctx context.Context) (string, error)  // https://docs.skytable.io/actions/sys#info
	SysInfoProtoVer(ctx context.Context) (float32, error) // https://docs.skytable.io/actions/sys#info
	// SysMetricHealth (ctx context.Context) (string, error)
	// SysMetricStorage (ctx context.Context) (uint64, error)

	MKSnap (ctx context.Context, name string) error // https://docs.skytable.io/actions/mksnap
}

type SkytablePool interface {
	Skytable

	RentConn(dontOpenNew bool) (conn *Conn, pusher func(), err error)
}
