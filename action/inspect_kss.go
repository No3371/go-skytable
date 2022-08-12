package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// Get a list of all keyspaces within the instance.
type InspectKeyspaces struct {}


// Get a list of all keyspaces within the instance.
//
// ⚠️ Only use this when sending packets contains this action only.
func FormatInspectKeyspaces () string {
	return "*1\n~2\n7\nINSPECT\n9\nKEYSPACES\n"
}

func (q InspectKeyspaces) AppendToPacket(builder *strings.Builder) error {
	_, err := builder.WriteString("~2\n7\nINSPECT\n9\nKEYSPACES\n")
	return err
}

func (q InspectKeyspaces) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case *protocol.TypedArray:
		if response.ArrayType != protocol.CompoundTypeTypedArray {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspaces: Unexpected array type: %s", response.ArrayType), nil)
		} else {
			return nil
		}
	case protocol.ResponseCode:
		switch response {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspaces: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("InspectKeyspaces: Unexpected response element: %v", response), nil)
	}
}
