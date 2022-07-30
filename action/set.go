package action

import (
	"log"
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
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	AppendElement("SET", builder, false)
	AppendElement(q.key, builder, false)
	log.Printf("sending key length: %d", len(q.key))
	AppendElement(q.value, builder, false)
	log.Printf("sending value length: %d (string)", len(q.value.(string)))
	log.Printf("sending value length: %d (bytes)", len([]byte(q.value.(string))))
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
			return protocol.ErrUnexpectedProtocol
		}
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
