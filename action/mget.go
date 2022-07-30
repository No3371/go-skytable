package action

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type MGet struct {
	keys []string
}

func NewMGet(keys []string) *MGet {
	return &MGet{
		keys: keys,
	}
}

func (q MGet) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.keys) + 1, builder)
	AppendElement("MGet", builder, false)
	for _, k := range q.keys {
		AppendElement(k, builder, false)
	}
	return nil
}

func (q MGet) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case protocol.TypedArray:
		return nil
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
