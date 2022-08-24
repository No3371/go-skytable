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
	case protocol.TypedArray:
		if resp.ArrayType == protocol.CompoundTypeTypedNonNullArray {
			return nil
		} else {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("WhereAmI: Unexpected array type: %v", resp.ArrayType), nil)
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
