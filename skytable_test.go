package skytable_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/protocol"
)

// func TestTCP (t *testing.T) {
//     sChan := make(chan struct{}, 1)
//     fChan := make(chan error, 1)
//     go func () {
//         lsr, err := net.ListenTCP("tcp", &net.TCPAddr{ IP: []byte{ 127, 0, 0, 1 }, Port: 61819})
//         if err != nil {
//             t.Error(err)
//             fChan<-err
//             return
//         }
//         conn, err := lsr.AcceptTCP()
//         if err != nil {
//             t.Error(err)
//             fChan<-err
//             return
//         }
//         read, err := io.ReadAll(conn)
//         if err != nil {
//             t.Error(err)
//             fChan<-err
//             return
//         }
//         t.Log("TCP received")
//         t.Log(string(read))
//         t.Log(len(read))
//         t.Log(string(read))
//         t.Log("TCP received")
//         conn.Write([]byte("RECEIVED"))
//         sChan<-struct{}{}
//     } ()

//     <-time.NewTimer(time.Second).C

// 	c, err := skytable.NewClient(context.Background(), &net.TCPAddr{ IP: []byte{ 127, 0, 0, 1 }, Port: 61819})
//     if err != nil {
//         t.Fatal(err)
//     }

//     go func () {
//         <-time.NewTimer(time.Second).C
//         c.Close()
//     } ()

//     _ = c.AuthLogin("user", "token")

//     select {
//     case <-sChan:
//     case err := <-fChan:
//         t.Fatal(err)
//     }
// }

func TestConnLocal(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
	c, err := skytable.NewClient(ctx, &net.TCPAddr{ IP: []byte{ 127, 0, 0, 1 }, Port: int(protocol.DefaultPort)})
    if err != nil {
        t.Fatal(err)
    }

    t.Log("Connected!")

    token, foundToken := os.LookupEnv("GO_SKYTABLE_TEST_TOKEN")
    if !foundToken {
        t.Fatal("GO_SKYTABLE_TEST_TOKEN not found in env")
    }

    err = c.AuthLogin("go-skytable-test", token)
    if err != nil {
        t.Fatal(err)
    }

    t.Log("Authenticated!")

    k := "t1233 あ得"
    v := "り8しれ 工さ小"

    existed, err := c.Exists([]string { k })
    if err != nil {
        t.Fatal(err)
    } else if existed > 0 {
        deleted, err := c.Del([]string { k })
        if err != nil {
            t.Fatal(err)
        } else if deleted != 1 {
            t.Fatalf("Deleted %d, expecting %d", deleted, existed)
        }
    }

    err = c.SetString(k, v)
    if err != nil {
        t.Fatal(err)
    }

    t.Log("SET sent!")

    respV, err := c.GetString(k)
    if err != nil {
        t.Fatal(err)
    }

    t.Log("GET done!")

    t.Log("SET: " + v)
    t.Log("GET: " + respV)
    if respV != v {
        t.Fail()
    }
}