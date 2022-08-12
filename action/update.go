package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Update struct {
	key   string
	value interface{}
}

func NewUpdate(key string, value interface{}) *Update {
	return &Update{
		key:   key,
		value: value,
	}
}

func (q Update) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "UPDATE", q.key, q.value)
	if err != nil {
		return err
	}

	return nil
}

func (q Update) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Login: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Update: Unexpected response element: %v", response), nil)
	}
}
