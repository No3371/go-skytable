package protocol

import (
	"fmt"
	"testing"
)

func TestDDLDataTypes_String(t *testing.T) {
	tests := []struct {
		name string
		dt   DDLDataTypes
		want string
	}{
		{ "String", DDLDataTypes_String, "_str" },
		{ "BinaryString", DDLDataTypes_BinaryString, "_binstr" },
		{ "List", DDLDataTypes_List, "_list" },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fmt.Sprintf("_%s", tt.dt); got != tt.want {
				t.Errorf("DDLDataTypes.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
