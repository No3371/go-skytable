package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// If Path is left empty, "INSPECT KEYSPACE" will be sent (inspect current keyspace)
type InspectKeyspace struct {
	Path string
}

func NewInspectKeyspace(path string) *InspectKeyspace {
	return &InspectKeyspace{
		Path: path,
	}
}

func FormatInspectKeyspace (path string) string {
	return fmt.Sprintf("*1\n~3\n4\nINSPECT\n8\nKEYSPACE\n%d\n%s\n", len(path), path)
}

func (q InspectKeyspace) AppendToPacket(builder *strings.Builder) error {
	if strings.Contains(q.Path, ":") {
		return errors.New("do not include : in the path when Inspecting keyspace")
	}

	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	AppendElement("Inspect", builder, false)
	AppendElement("KEYSPACE", builder, false)
	AppendElement(q.Path, builder, false)
	return nil
}

func (q InspectKeyspace) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespErrStr:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspace: Unexpected response element: %v", response), nil)
	}
}
