package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// Note:
// KEYSPACE for Keyspaces
// KEYSPACE:TABLE for tables


type Use struct {
	path string
}

func NewUse(path string) *Use {
	return &Use{
		path: path,
	}
}

func FormatUse (path string) string {
	return fmt.Sprintf("*1\n~2\n3\nUSE\n%d\n%s\n", len(path), path)
}

func (q Use) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	AppendElement("USE", builder, false)
	AppendElement(q.path, builder, false)
	return nil
}

func (q Use) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Use: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Use: Unexpected response element: %v", response), nil)
	}
}
