package skytable_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/action"
	"github.com/No3371/go-skytable/protocol"
)

func TestConn_ExecSingleActionPacketRaw(t *testing.T) {
	c, err := NewConnNoAuth()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		args    []any
		want    any
		wantErr bool
	}{
		{"SetX", []any{"SET", "X", 100}, protocol.RespOkay, false},
		{"SysInfoProtocol", []any{"SYS", "INFO", "PROTOCOL"}, skytable.ProtoVer, false},
		{"DelX", []any{"DEL", "X"}, uint64(1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.ExecSingleActionPacketRaw(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Conn.ExecSingleActionPacketRaw() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Value, tt.want) {
				t.Errorf("Conn.ExecSingleActionPacketRaw() = %v, want %v", got.Value, tt.want)
			}
		})
	}
}

func TestConn_USet(t *testing.T) {
	c, err := NewConnNoAuth()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		entries    []action.KVPair
		wantSet uint64
		wantDel uint64
		wantErr bool
	}{
		{ "1", []action.KVPair{ { K: "A", V: 100 } }, 1, 1, false },
		{ "2", []action.KVPair{ { K: "A", V: 100 }, { K: "B", V: 1000 } }, 2, 2, false },
		{ "3", []action.KVPair{ { K: "A", V: 100 }, { K: "A", V: 1000 }, { K: "A", V: 10000 } }, 3, 1, false },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSet, err := c.USet(context.Background(), tt.entries...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Conn.USet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotSet != tt.wantSet {
				t.Errorf("Conn.USet() = %v, want %v", gotSet, tt.wantSet)
			}

			toDel := make([]string, len(tt.entries))
			for i := 0; i < len(tt.entries); i++ {
				toDel[i] = tt.entries[i].K
			}

			deleted, err := c.Del(context.Background(), toDel)
			if err != nil {
				t.Fatal(err)
			}

			if deleted != tt.wantDel {
				t.Fatal("failed to clear up by Del")
			}
		})
	}
}
