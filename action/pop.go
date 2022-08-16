package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Pop struct {
	Key string
}

func (q Pop) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "POP", q.Key)
	if err != nil {
		return err
	}

	return nil
}

func (q Pop) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Pop: Unexpected response element: %v", response), nil)
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Pop: Unexpected response element: %v", response), nil)
	}
}
