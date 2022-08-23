package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// If Path is left empty, "INSPECT KEYSPACE" will be sent (inspect current keyspace)
type InspectKeyspace struct {
	Name string
}

func FormatSingleInspectKeyspacePacket(name string) string {
	if name == "" {
		return "*1\n~2\n7\nINSPECT\n8\nKEYSPACE\n"
	} else {
		return fmt.Sprintf("*1\n~3\n7\nINSPECT\n8\nKEYSPACE\n%d\n%s\n", len(name), name)
	}
}

func (q InspectKeyspace) AppendToPacket(builder *strings.Builder) error {
	if q.Name == "" {
		_, err := builder.WriteString("~2\n7\nINSPECT\n8\nKEYSPACE\n")
		if err != nil {
			return err
		}
	}

	if strings.Contains(q.Name, ":") {
		return errors.New("do not include : in the path when Inspecting keyspace")
	}

	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "INSPECT", "KEYSPACE", q.Name)
	if err != nil {
		return err
	}

	return nil
}

func (q InspectKeyspace) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case *protocol.TypedArray:
		if response.ArrayType != protocol.CompoundTypeTypedArray {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace: Unexpected array type: %s", response.ArrayType), nil)
		} else {
			return nil
		}
	case protocol.ResponseCode:
		switch response {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace: Unexpected response element: %v", response), nil)
	}
}
