# go-skytable

A Go driver of [Skytable](https://github.com/skytable/skytable).

## Status

The package implement Skyhash 1.1.

Tested with: Skytable 0.7.5.

No DDL supports yet so it can only works with `default:default` (keymap<string, binarystr>).

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
- [ ] Float
- [ ] Json

- [X] TypedArray
- [ ] Array
- [ ] FlatArray
- [ ] AnyArray
- [ ] TypedNonNullArray

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