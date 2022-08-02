package skytable_test

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
)

const testUserName = "go-skytable-test"

func GetTestToken() (string, bool) {
	token, foundToken := os.LookupEnv("GO_SKYTABLE_TEST_TOKEN")
	if foundToken {
		return token, true
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	log.Printf("Reading: %s", wd+"\\go-skytable-test")
	read, err := os.ReadFile(wd + "\\go-skytable-test")
	if err != nil {
		return string(read), false
	}

	log.Printf("Test user: %s %s", testUserName, read)
	return string(read), true
}

func TestConnPoolLocalAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	localAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}
	c := skytable.NewConnPool(localAddr, skytable.ConnPoolOptions{
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

func TestConnPoolLocalSetGetBurst(t *testing.T) {
    bursts := []int{ 144, 256, 1024 }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	token, gotToken := GetTestToken()
	if !gotToken {
		t.Fatalf("failed to get token of" + testUserName)
	}

	localAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}
    
	c := skytable.NewConnPool(localAddr, skytable.ConnPoolOptions{
		AuthProvider: func() (u, t string) {
			u = testUserName
			t = token
			return u, t
		},
	})

	k := "t1233 あ得"
	v := "り8しれ 工さ小"

	existed, err := c.Exists(ctx, []string{k})
	if err != nil {
		t.Fatal(err)
	} else if existed > 0 {
		deleted, err := c.Del(ctx, []string{k})
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

    for _, b := range bursts {
        sTime := time.Now()
        errChan := make(chan error)
        wg := &sync.WaitGroup{}
        for i := 0; i < b; i++ {
            wg.Add(1)
            go func () {
                defer wg.Done()
                respV, err := c.GetBytes(ctx, k)
                if err != nil {
                    errChan<-err
                    close(errChan)
                } else if string(respV) != v {
                    errChan<-errors.New("result mismatch")
                    close(errChan)
                }
            } ()
        }
        wg.Wait()
        t.Logf("1st GETs (x%d): %s", b, time.Since(sTime))
        select {
        case e := <-errChan:
            t.Fatal(e)
        default:
        }

        errChan = make(chan error)
        sTime = time.Now()
        for i := 0; i < b; i++ {
            wg.Add(1)
            go func () {
                defer wg.Done()
                respV, err := c.GetBytes(ctx, k)
                if err != nil {
                    errChan<-err
                    close(errChan)
                } else if string(respV) != v {
                    errChan<-errors.New("result mismatch")
                    close(errChan)
                }
            } ()
        }
        wg.Wait()
        t.Logf("2nd GETs (x%d): %s", b, time.Since(sTime))
        select {
        case e := <-errChan:
            t.Fatal(e)
        default:
        }
    }
}

func TestConnLocalAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

    auth := func() (username, token string) {
        t, gotToken := GetTestToken()
        if !gotToken {
            panic("failed to get token of" + testUserName)
        }
        return testUserName, t
    }

	c, err := skytable.NewConnAuth(&net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}, auth)
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

func TestConnLocalSetSeqGet(t *testing.T) {
    seqSize := []int { 16, 32, 64, 128, 1024 }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	token, gotToken := GetTestToken()
	if !gotToken {
		t.Fatalf("failed to get token of '%s'", testUserName)
	}

    auth := func() (u, t string) {
            u = testUserName
            t = token
            return u, t
        }

	c, err := skytable.NewConnAuth(&net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}, auth)
	if err != nil {
		t.Fatal(err)
	}

	err = c.AuthLogin(ctx, testUserName, token)
	if err != nil {
		t.Fatal(err)
	}

	k := "t1233 あ得"
	v := "り8しれ 工さ小"

	existed, err := c.Exists(ctx, []string{k})
	if err != nil {
		t.Fatal(err)
	} else if existed > 0 {
		deleted, err := c.Del(ctx, []string{k})
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

    var sTime time.Time
    for _, seq := range seqSize {
        sTime = time.Now()
        for i := 0; i < seq; i++ {
            _, err := c.GetBytes(ctx, k) 
            if err != nil {
                t.Fatal(err)
            }
        }
        t.Logf("GETs (x%d): %s", seq, time.Since(sTime))
    }
}


func TestConnLocalSetMGet(t *testing.T) {
    seqSize := []int { 64, 512, 1024, 4096 }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	token, gotToken := GetTestToken()
	if !gotToken {
		t.Fatalf("failed to get token of '%s'", testUserName)
	}

    auth := func() (u, t string) {
            u = testUserName
            t = token
            return u, t
        }

	c, err := skytable.NewConnAuth(&net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}, auth)
	if err != nil {
		t.Fatal(err)
	}

	err = c.AuthLogin(ctx, testUserName, token)
	if err != nil {
		t.Fatal(err)
	}

	k := "t1233 あ得"
	v := "り8しれ 工さ小"

	existed, err := c.Exists(ctx, []string{k})
	if err != nil {
		t.Fatal(err)
	} else if existed > 0 {
		deleted, err := c.Del(ctx, []string{k})
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

    keys := make([]string, seqSize[len(seqSize) - 1])
    for j := 0; j < len(keys); j++ {
        keys[j] = k
    }

    var sTime time.Time
    for _, seq := range seqSize {
        if seq > len(keys) {
            keys := make([]string, seq)
            for j := 0; j < len(keys); j++ {
                keys[j] = k
            }
        }

        p := skytable.NewQueryPacket(
            []skytable.Action{
                action.NewMGet(keys[:seq]),
        })
    
        sTime = time.Now()
        bq, err := c.BuildQuery(p)
        if err != nil {
            t.Fatal(err)
        }
        t.Logf("Building MGET (size: %d): %s", seq, time.Since(sTime))

        sTime = time.Now()
        rp, err := c.ExecQuery(bq)
        if err != nil {
            t.Fatal(err)
        }
        t.Logf("Executing MGET (size: %d): %s", seq, time.Since(sTime))

        if rp.Resps()[0].Err != nil {
            t.Fatal(rp.Resps()[0].Err)

        }
    }
}

func TestConnLocalExistsDelSetGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
    
	token, gotToken := GetTestToken()
	if !gotToken {
		t.Fatalf("failed to get token of '%s'", testUserName)
	}

    auth := func() (u, t string) {
            u = testUserName
            t = token
            return u, t
        }

	c, err := skytable.NewConnAuth(&net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(protocol.DefaultPort)}, auth)
	if err != nil {
		t.Fatal(err)
	}

	err = c.AuthLogin(ctx, testUserName, token)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Authenticated!")

	k := "t1233 あ得"
	v := "り8しれ 工さ小"

    c.Err()

	existed, err := c.Exists(ctx, []string{k})
	if err != nil {
		t.Fatal(err)
	} else if existed > 0 {
		deleted, err := c.Del(ctx, []string{k})
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
