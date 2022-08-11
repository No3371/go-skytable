# go-skytable

![](https://goreportcard.com/badge/github.com/No3371/go-skytable)

A Go driver of [Skytable](https://github.com/skytable/skytable).

## Status

The package implement Skyhash 1.1.

Tested with: Skytable 0.7.5.

No DDL supports yet so it can only works with `default:default` (keymap<string, binarystr>).

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
auth := func() (u, t string) {
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
auth := func() (u, t string) {
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

## Implemented

### Mechanics

- [ ] TLS
- [ ] DDL (Keyspaces/Tables)

### DataTypes

- [X] ResponseCode
- [X] Integer
- [X] SignedInteger
- [X] String
- [X] BinaryString
- [ ] SmallInteger
- [ ] SignedSmallInteger
- [X] Float
- [ ] Json

- [X] TypedArray
- [ ] Array
- [ ] FlatArray
- [ ] AnyArray
- [X] TypedNonNullArray

### Actions

- [X] AUTH LOGIN
- [ ] AUTH CLAIM
- [ ] AUTH LOGOUT
- [ ] AUTH ADDUSER
- [ ] AUTH DELUSER
- [ ] AUTH RESTORE
- [ ] AUTH LISTUSER
- [ ] AUTH WHOAMI

- [X] (RAW QUERY)

- [X] GET
- [X] SET
- [X] UPDATE
- [X] MGET
- [X] DEL
- [X] EXISTS
- [X] HEYA
- [ ] POP
- [ ] MPOP
- [ ] MSET
- [ ] MUPDATE
- [ ] SDEL
- [ ] SSET
- [ ] USET
- [ ] LGET
- [ ] LMOD
- [ ] LGET
- [ ] LSET
- [ ] LSKEYS

- [ ] DBSIZE
- [ ] FLUSHDB
- [ ] KEYLEN
- [ ] WHEREAMI

- [ ] MKSNAP

- [X] SYS INFO VERSION
- [X] SYS INFO PROTOCOL
- [ ] SYS INFO PROTOVER
- [ ] SYS METRIC HEALTH
- [ ] SYS METRIC STORAGE

## DDL Actions

- [X] CREATE KEYSPACE
- [X] USE KEYSPACE
- [ ] INSPECT KEYSPACE
- [X] DROP KEYSPACE

- [X] CREATE TABLE
- [X] USE TABLE
- [ ] INSPECT TABLE
- [X] DROP TABLE

## DDL with Connection Pool

Connection Pools manage multiple connections on its own and users have no way to decide which `Conn` is used on method calls. Running `USE` with Connection Pools means a random `Conn` will use the specified container.

This means it's possible for connections in a pool using different container, therefore actions could be unintentionally sent to unexpected containers.

If you are working with multiple Keyspaces/Tables and you are using Connection Pool, there are 2 suggested usages:

- **Container-dedicated connection pool**: Calling `*ConnPool.UseKeyspace/Table` will iterate through all opened connections in the pool and call their USE, and change the default Keyspace/Table of the future new connection in the pool.
- **USE first in every packet**: this may introduce performance loss and frequent USEs are not recommended by Skytable official.

## Testing

Testcases are written for both Auth-Enabled and Auth-Disabled Skytable instances.
The Auth-Enabled one should be bound to 2003 (Skytable default port), while the Auth-Disabled one should be bound to 2004 (as specified in `skytable_test.go`).

All auth testcases use username `go-skytable-test` (as specified in `skytable_test.go`), and looks up the token by:

1. Read the value of environment variable `GO_SKYTABLE_TEST_TOKEN` as the token.
2. If step 1 failed, read a file in the repo named `go-skytable-test` and read the content as the token.

If the `go-skytable-test` user and the token are setup correctly, the auth testcases should run without issues.