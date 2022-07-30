package skytable_test

import (
	"context"
	"log"
	"net"
	"os"
	"testing"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/protocol"
)

const testUserName = "go-skytable-test"

func GetTestToken () (string, bool) {
    token, foundToken := os.LookupEnv("GO_SKYTABLE_TEST_TOKEN")
    if foundToken {
        return token, true
    }

    wd, err := os.Getwd()
    if err != nil {
        return "", false
    }
    log.Printf("Reading: %s", wd + "\\go-skytable-test")
    read, err := os.ReadFile(wd + "\\go-skytable-test")
    if err != nil {
        return string(read), false
    }

    log.Printf("Test user: %s %s", testUserName, read)
    return string(read), true
}

func TestConnPoolLocalAuth (t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

	localAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}
	c := skytable.NewConnPool(localAddr, skytable.ConnPoolOptions{
        Cap: 16,
        AuthProvider: func() (username, token string) {
            t, gotToken := GetTestToken()
            if !gotToken {
                panic("failed to get token of" + testUserName)
            }
            return testUserName, t
        },
    })

    token, gotToken := GetTestToken()
    if !gotToken {
        t.Fatalf("failed to get token of" + testUserName)
    }

    err := c.AuthLogin(ctx, testUserName, token)
    if err != nil {
        t.Fatal(err)
    }
}

func TestConnLocalAuth (t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
	c, err := skytable.NewConn(&net.TCPAddr{ IP: []byte{ 127, 0, 0, 1 }, Port: int(protocol.DefaultPort)})
    if err != nil {
        t.Fatal(err)
    }

    token, gotToken := GetTestToken()
    if !gotToken {
        t.Fatalf("failed to get token of '%s'", testUserName)
    }

    err = c.AuthLogin(ctx, testUserName, token)
    if err != nil {
        t.Fatal(err)
    }
}

func TestConnLocalExistsDelSetGet(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
	c, err := skytable.NewConn(&net.TCPAddr{ IP: []byte{ 127, 0, 0, 1 }, Port: int(protocol.DefaultPort)})
    if err != nil {
        t.Fatal(err)
    }

    t.Log("Connected!")

    token, gotToken := GetTestToken()
    if !gotToken {
        t.Fatalf("failed to get token of '%s'", testUserName)
    }

    err = c.AuthLogin(ctx, testUserName, token)
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

    err = c.Set(ctx, k, v)
    if err != nil {
        t.Fatal(err)
    }

    t.Log("SET sent!")

    respV, err := c.GetBytes(ctx, k)
    if err != nil {
        t.Fatal(err)
    }

    t.Log("GET done!")

    t.Log("SET: " + v)
    t.Log("GET: " + string(respV))
    if string(respV) != v {
        t.Fail()
    }
}