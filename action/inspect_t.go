package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// If Path is left empty, "INSPECT TABLE" will be sent (inspect current table)
type InspectTable struct {
	Path string
}

// If Path is left empty, "INSPECT TABLE" will be sent (inspect current table)
//
// ⚠️ Only use this when sending packets contains this action only.
func FormatSingleInspectTablePacket(path string) string {
	if path == "" {
		return "*1\n~2\n7\nINSPECT\n5\nTABLE\n"
	} else {
		return fmt.Sprintf("*1\n~3\n7\nINSPECT\n5\nTABLE\n%d\n%s\n", len(path), path)
	}
}

func (q InspectTable) AppendToPacket(builder *strings.Builder) error {
	if q.Path == "" {
		_, err := builder.WriteString("~2\n7\nINSPECT\n5\nTABLE\n")
		if err != nil {
			return err
		}
	}

	if !strings.Contains(q.Path, ":") {
		return errors.New("use explicit full path to the table to inspect it (keyspace:table)")
	}

	fmt.Fprintf(builder, "~3\n7\nINSPECT\n5\nTABLE\n%d\n%s\n", len(q.Path), q.Path)
	return nil
}

func (q InspectTable) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case string:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespErrStr:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectTable: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectTable: Unexpected response element: %v", response), nil)
	}
}
