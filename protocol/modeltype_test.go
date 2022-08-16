package protocol

import (
	"reflect"
	"testing"
)

func TestParseDescription(t *testing.T) {
	type args struct {
		descStr string
	}
	tests := []struct {
		name    string
		args    args
		want    ModelDescription
		wantErr bool
	}{
		{ "bin,bin", args { "Keymap { data: (binstr,binstr), volatile: true }" },
		  KeyMapDescription { DDLDataTypes_BinaryString, DDLDataTypes_BinaryString, true }, false },
		{ "bin,str", args { "Keymap { data: (binstr,str), volatile: false }" },
		  KeyMapDescription { DDLDataTypes_BinaryString, DDLDataTypes_String, false }, false },
		{ "str,bin", args { "Keymap { data: (str,binstr), volatile: true }" },
		  KeyMapDescription { DDLDataTypes_String, DDLDataTypes_BinaryString, true }, false },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDescription(tt.args.descStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDescription() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseDescription() = %v, want %v", got, tt.want)
			}
		})
	}
}
