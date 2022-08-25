package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type WhereAmI struct {}

func (q WhereAmI) AppendToPacket(builder *strings.Builder) (err error) {
	_, err = builder.WriteString("~1\n8\nWHEREAMI\n")
	return err
}

func (q WhereAmI) ValidateProtocol(response interface{}) error {
	switch resp := response.(type) {
	case *protocol.TypedArray:
		if resp.ArrayType != protocol.CompoundTypeTypedNonNullArray {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI: Unexpected array type: %v", resp.ArrayType), nil)
		} else if resp.ElementType != protocol.SimpleTypeString {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI: Unexpected array element type: %v", resp.ElementType), nil)
		} else if len(resp.Elements) > 2 || len (resp.Elements) == 0 {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI: Unexpected array size: %d, (%v)", len(resp.Elements), resp.Elements), nil)
		} else {
			return nil
		}
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI: Unexpected response code: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI: Unexpected response element: %v", response), nil)
	}
}
