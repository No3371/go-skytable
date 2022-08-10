package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// Note:
// KEYSPACE for Keyspaces
// KEYSPACE:TABLE for tables



type CreateKeyspace struct {
	Path string
}

func FormatCreateKeyspace (path string) string {
	return fmt.Sprintf("*1\n~2\n3\nCREATE\n8\nKEYSPACE\n%d\n%s\n", len(path), path)
}

func (q CreateKeyspace) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	AppendElement("CREATE", builder, false)
	AppendElement("KEYSPACE", builder, false)
	AppendElement(q.Path, builder, false)
	return nil
}

func (q CreateKeyspace) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Create: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Create: Unexpected response element: %v", response), nil)
	}
}
