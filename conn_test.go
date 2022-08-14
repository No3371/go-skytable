package skytable_test

import "testing"

func TestConn_BuildSingleActionPacketRaw(t *testing.T) {
	c, err := NewConnNoAuth()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		args    []string
		wantRaw string
		wantErr bool
	}{
		{ "GetX", []string { "GET", "X" }, "*1\n~2\n3\nGET\n1\nX\n", false },
		{ "SysInfoProtocol", []string { "SYS", "INFO", "PROTOCOL" }, "*1\n~3\n3\nSYS\n4\nINFO\n8\nPROTOCOL\n", false },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRaw, err := c.BuildSingleActionPacketRaw(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Conn.BuildSingleActionPacketRaw() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotRaw != tt.wantRaw {
				t.Errorf("Conn.BuildSingleActionPacketRaw() = %v, want %v", gotRaw, tt.wantRaw)
			}
		})
	}
}
