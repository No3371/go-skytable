package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Set struct {
	key   string
	value interface{}
}

func NewSet(key string, value interface{}) *Set {
	return &Set{
		key:   key,
		value: value,
	}
}

func (q Set) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "SET", q.key, q.value)
	if err != nil {
		return err
	}
	return nil
}

func (q Set) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespOverwriteError:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SET: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SET: Unexpected response element: %v", response), nil)
	}
}
