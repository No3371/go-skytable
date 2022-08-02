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
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	AppendElement("UPDATE", builder, false)
	AppendElement(q.key, builder, false)
	AppendElement(q.value, builder, false)
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
