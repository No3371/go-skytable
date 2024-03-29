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
	// https://docs.skytable.io/actions/heya
	Heya(ctx context.Context, echo string) error
	// https://docs.skytable.io/actions/auth#login
	AuthLogin(ctx context.Context, authProvider AuthProvider) error
	// https://docs.skytable.io/actions/auth#logout
	AuthLogout(ctx context.Context) error
	// https://docs.skytable.io/actions/auth#claim
	AuthClaim(ctx context.Context, originKey string) (string, error)
	// https://docs.skytable.io/actions/auth#adduser
	AuthAddUser(ctx context.Context, username string) (string, error)
	// https://docs.skytable.io/actions/auth#deluser
	AuthDelUser(ctx context.Context, username string) error
	// https://docs.skytable.io/actions/auth#restore
	//
	// If provided `originKey` is "", it'll be omitted in the sent command
	AuthRestore(ctx context.Context, originKey string, username string) (string, error)
	// https://docs.skytable.io/actions/auth#listuser
	AuthListUser(ctx context.Context) (*protocol.TypedArray, error)
	// https://docs.skytable.io/actions/auth#whoami
	AuthWhoAmI(ctx context.Context) (string, error)

	// https://docs.skytable.io/actions/exists
	Exists(ctx context.Context, keys []string) (existing uint64, err error)
	// https://docs.skytable.io/actions/del
	Del(ctx context.Context, keys []string) (deleted uint64, err error)
	// https://docs.skytable.io/actions/sdel
	SDel(ctx context.Context, keys []string) error

	// https://docs.skytable.io/actions/get
	Get(ctx context.Context, key string) (response.ResponseEntry, error)
	// a strict version of [Get] that only success if the value is stored as String in Skytable.
	GetString(ctx context.Context, key string) (string, error)
	// a strict version of [Get] that only success if the value is stored as BinaryString in Skytable.
	GetBytes(ctx context.Context, key string) ([]byte, error)
	// https://docs.skytable.io/actions/mget
	MGet(ctx context.Context, keys []string) (*protocol.TypedArray, error)
	// https://docs.skytable.io/actions/pop
	Pop(ctx context.Context, key string) (response.ResponseEntry, error)
	// https://docs.skytable.io/actions/mpop
	MPop(ctx context.Context, keys []string) (*protocol.TypedArray, error)

	// https://docs.skytable.io/actions/set
	Set(ctx context.Context, key string, value any) error
	// https://docs.skytable.io/actions/mset
	MSetB(ctx context.Context, keys []string, values []any) (set uint64, err error)
	// https://docs.skytable.io/actions/mset
	MSet(ctx context.Context, entries []action.KVPair) (set uint64, err error)
	// https://docs.skytable.io/actions/sset
	SSet(ctx context.Context, entries []action.KVPair) error
	USet(ctx context.Context, entries ...action.KVPair) (set uint64, err error)

	// https://docs.skytable.io/actions/update
	Update(ctx context.Context, key string, value any) error
	// https://docs.skytable.io/actions/mupdate
	MUpdate(ctx context.Context, entries []action.KVPair) (updated uint64, err error)
	// https://docs.skytable.io/actions/supdate
	SUpdate(ctx context.Context, entries []action.KVPair) error

	// https://docs.skytable.io/actions/lget#lget
	LGet(ctx context.Context, listName string) (*protocol.TypedArray, error)
	// https://docs.skytable.io/actions/lget#limit
	LGetLimit(ctx context.Context, listName string, limit uint64) (*protocol.TypedArray, error)
	// https://docs.skytable.io/actions/lget#len
	LGetLen(ctx context.Context, listName string) (uint64, error)
	// https://docs.skytable.io/actions/lget#valueat
	LGetValueAt(ctx context.Context, listName string, index uint64) (response.ResponseEntry, error)
	// https://docs.skytable.io/actions/lget#first
	LGetFirst(ctx context.Context, listName string) (response.ResponseEntry, error)
	// https://docs.skytable.io/actions/lget#last
	LGetLast(ctx context.Context, listName string) (response.ResponseEntry, error)
	// https://docs.skytable.io/actions/lget#range
	//
	// If provided `to` is 0, it's omitted in the sent command.
	LGetRange(ctx context.Context, listName string, from uint64, to uint64) (*protocol.TypedArray, error)

	// https://docs.skytable.io/actions/lmod#push
	LModPush(ctx context.Context, listName string, elements []any) error
	// https://docs.skytable.io/actions/lmod#insert
	LModInsert(ctx context.Context, listName string, index uint64, element any) error
	// https://docs.skytable.io/actions/lmod#pop
	LModPop(ctx context.Context, listName string) (response.ResponseEntry, error)
	// https://docs.skytable.io/actions/lmod#pop
	LModPopIndex(ctx context.Context, listName string, index uint64) (response.ResponseEntry, error)
	// https://docs.skytable.io/actions/lmod#remove
	LModRemove(ctx context.Context, listName string, index uint64) error
	// https://docs.skytable.io/actions/lmod#clear
	LModClear(ctx context.Context, listName string) error

	// https://docs.skytable.io/actions/lset
	//
	// If `elements` is nil, it's omitted in the sent command.`
	LSet(ctx context.Context, listName string, elements []any) error

	Exec(packet *QueryPacket) ([]response.ResponseEntry, error)
	ExecSingleActionPacketRaw(segments ...any) (response.ResponseEntry, error)

	// https://docs.skytable.io/ddl/#use
	//
	// ``USE KEYSPACE'' and ``USE TABLE'' are unified into ``USE''.
	Use(ctx context.Context, path string) error

	// https://docs.skytable.io/ddl/#inspect
	InspectKeyspaces(ctx context.Context) (*protocol.TypedArray, error)
	// https://docs.skytable.io/ddl/#keyspaces
	CreateKeyspace(ctx context.Context, name string) error
	// https://docs.skytable.io/ddl/#keyspaces-1
	DropKeyspace(ctx context.Context, name string) error
	// https://docs.skytable.io/ddl/#keyspaces-2
	//
	// If name is "", inspect the current keyspace
	InspectKeyspace(ctx context.Context, name string) (*protocol.TypedArray, error)

	// https://docs.skytable.io/ddl/#tables
	CreateTable(ctx context.Context, path string, modelDesc any) error
	// https://docs.skytable.io/ddl/#tables-1
	DropTable(ctx context.Context, path string) error
	// https://docs.skytable.io/ddl/#tables-2
	//
	// If path is "", inspect the current table
	InspectTable(ctx context.Context, path string) (protocol.ModelDescription, error)

	// https://docs.skytable.io/actions/sys#info
	SysInfoVersion(ctx context.Context) (string, error)
	// https://docs.skytable.io/actions/sys#info
	SysInfoProtocol(ctx context.Context) (string, error)
	// https://docs.skytable.io/actions/sys#info
	SysInfoProtoVer(ctx context.Context) (float32, error)
	// https://docs.skytable.io/actions/sys#metric
	//
	// Returns true if "good", false when "critical"
	SysMetricHealth(ctx context.Context) (bool, error)
	// https://docs.skytable.io/actions/sys#metric
	SysMetricStorage(ctx context.Context) (uint64, error)

	// https://docs.skytable.io/actions/mksnap
	//
	// If name is "", it will only send "MKSNAP"
	MKSnap(ctx context.Context, name string) error
	// https://docs.skytable.io/actions/whereami
	WhereAmI(ctx context.Context) (string, error)
	// https://docs.skytable.io/actions/dbsize
	//
	// If entity is "", check the current table
	DBSize(ctx context.Context, entity string) (uint64, error)
	// https://docs.skytable.io/actions/keylen
	KeyLen(ctx context.Context, key string) (uint64, error)
	// https://docs.skytable.io/actions/flushdb
	//
	// If entity is "", flush the current table
	FlushDB(ctx context.Context, entity string) error

	// https://docs.skytable.io/actions/lskeys
	LSKeys(ctx context.Context, entity string, limit uint64) (*protocol.TypedArray, error)
}

type SkytablePool interface {
	Skytable

	RentConn(dontOpenNew bool) (conn *Conn, pusher func(), err error)
}
