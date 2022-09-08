# go-skytable

[![Go Reference](https://pkg.go.dev/badge/github.com/No3371/go-skytable.svg)](https://pkg.go.dev/github.com/No3371/go-skytable)

A Go driver of [Skytable](https://github.com/skytable/skytable), a fast, secure and reliable realtime NoSQL database.

## Status

The package implements Skyhash 1.1.

DDL actions implemented.

The interfaces may be changed anytime before first release.

## Installation

```
go get github.com/No3371/go-skytable
```

## Usage

**Open single connection to a local Skytable instance**

```go
localAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}

// Auth is disabled in the instance
c, err := skytable.NewConn(localAddr)
// or
auth := func() (u, t string) { // ⚠️ You don't really write AuthProvider like this! Never include your credential in code!
        u = "USERNAME"
        t = "TOKEN"
        return u, t
    }
c, err := skytable.NewConnAuth(localAddr, auth)
```

**Open a connection pool to a local Skytable instance**
```go
localAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}

// Auth is disabled in the instance
c := skytable.NewConnPool(localAddr, skytable.DefaultConnPoolOptions)
// or
auth := func() (u, t string) { // ⚠️ You don't really write AuthProvider like this! Never include your credential in code!
        u = "USERNAME"
        t = "TOKEN"
        return u, t
    }
c := skytable.NewConnPool(localAddr, skytable.ConnPoolOptions{
    AuthProvider: auth,
})
```

**Set a value**
```go
err := c.Set(ctx, "KEY", "VALUE")
```

**Get a value**
```go
resp, err := c.Get(ctx, "KEY")
```

**Multi-actions query**
```go
p := skytable.NewQueryPacket(
    []skytable.Action{
        action.NewDel([]string { "KEY" }),
        action.NewSet("KEY", "VALUE"),
        action.NewGet("KEY"),
})

resp, err := c.BuildAndExecQuery(p)
```

## Progress

### Mechanics

⬜ TLS
✅ DDL (Keyspaces/Tables)
✅ Auto-Reconnection

### DataTypes

| ✅ Implemented | ⬜ NotImplemented | 🟪 WaitingForSkyhash  |   |    |
| --- | --- | --- | --- | --- |
| --- | --- | --- | --- | --- |
| ✅ ResponseCode | ✅ Integer | ✅ SignedInteger | ✅ String | ✅ BinaryString |
| ✅ Float | ⬜ SmallInteger | ⬜ SignedSmallInteger | 🟪 Json |  |
| ⬜ Array | ✅ FlatArray | ✅ AnyArray | ✅ TypedArray | ✅ TypedNonNullArray |

### Actions (Fully Supports Skytable 0.7.6)

| ✅ Implemented | ⬜ NotImplemented | 🟪 Partial  |   |    |
| --- | --- | --- | --- | --- |
| --- | --- | --- | --- | --- |
| ✅ GET | ✅ SET | ✅ UPDATE | ✅ MGET | ✅ MSET |
| ✅ DEL | ✅ EXISTS | ✅ HEYA | ✅ USET | ✅ POP |
| ✅ MPOP | ✅ MUPDATE | ✅ SDEL | ✅ SSET | ✅ SUPDATE |
| ✅ LMOD | ✅ LGET | ✅ LSET | ✅ LSKEYS | |
| ✅ DBSIZE | ✅ FLUSHDB | ✅ KEYLEN | ✅ WHEREAMI | ✅ MKSNAP |
| ✅ SYS | ✅ AUTH | | | |

## SkytableX

The subpackage provides opinionated extensions that could be useful or convenient.

For example, `*ConnX.GetWithSimTTL()`, `*ConnX.SetWithSimTTL()`, `*ConnX.UpdateWithSimTTL()` are alternative versions of their respective methods of Conn, these methods only works with []byte values and automatically add an action to maintain timestamp with key "key_timestamp".

## DDL with Connection Pool

Connection Pools manage multiple connections on its own and users have no way to decide which `Conn` is used on method calls. 

If you are working with multiple Keyspaces/Tables and you are using Connection Pool, there are 2 suggested usages:

- **Container-dedicated connection pool**: Keep a connection pool for every container. By specifying default container in ConnectionPoolOptions, all the new connections spawned by the pool automatically `USE` it. Running `USE` is equal to run `USE` on all of the existing connections in it, and change the default container of the pool so future connections will automatically `USE` that.
- **USE first in every packet**: This should explain itself, but it may introduce performance loss and frequent USEs are not recommended by Skytable official.

## Testing

Testcases are written for local Skytable instances (@127.0.0.1), some of them use auth coonnections, some don't.

The Auth-Enabled one should be bound to 2003 (Skytable default port), while the Auth-Disabled one should be bound to 2004 (as specified in `skytable_test.go`).

### Auth

All auth testcases use username `go-skytable-test` (as specified in `skytable_test.go`), and looks up the token by:

1. Read the value of environment variable `GO_SKYTABLE_TEST_TOKEN` as the token.
2. If step 1 failed, read a file in the repo named `go-skytable-test` and read the content as the token.

If the `go-skytable-test` user and the token are setup correctly, the auth testcases should run without issues.

## Known Issues:
- (Skytable) On Windows, executing DDL to an Auth-Enabled instance will results in auth data file loss. This has been reported and will be fixed in Skytable 0.7.6.
