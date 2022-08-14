package skytable_test

import (
	"reflect"
	"testing"

	"github.com/No3371/go-skytable"
	"github.com/No3371/go-skytable/protocol"
)

func TestConn_ExecSingleActionPacketRaw(t *testing.T) {
	c, err := NewConnNoAuth()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		args    []string
		want    any
		wantErr bool
	}{
		{ "SetX", []string { "SET", "X", "100" }, protocol.RespOkay, false },
		{ "SysInfoProtocol", []string { "SYS", "INFO", "PROTOCOL" }, skytable.ProtoVer, false },
		{ "DelX", []string { "DEL", "X" }, uint64(1), false },
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
