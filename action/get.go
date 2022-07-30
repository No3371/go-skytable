package action

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Get struct {
	key string
}

func NewGet(key string) *Get {
	return &Get{
		key: key,
	}
}

func (q Get) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	AppendElement("GET", builder, false)
	AppendElement(q.key, builder, false)
	return nil
}

func (q Get) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		default:
			return protocol.ErrUnexpectedProtocol
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
