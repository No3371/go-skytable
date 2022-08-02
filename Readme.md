# go-skytable

A Go driver of [Skytable](https://github.com/skytable/skytable).

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

**Get a value
```go
resp, err := c.Get(ctx, "KEY")
```

## Implemented

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

-[X] AUTH LOGIN
-[X] GET
-[X] SET
-[X] UPDATE
-[X] MGET
-[X] DEL
-[X] EXISTS
-[X] HEYA
-[X] (RAW QUERY)
-[ ] POP

-[X] SYS INFO VERSION
-[X] SYS INFO PROTOCOL
-[ ] SYS INFO PROTOVER
-[ ] SYS METRIC HEALTH
-[ ] SYS METRIC STORAGE
-[ ] (Keyspaces related)
-[ ] (Tables related)