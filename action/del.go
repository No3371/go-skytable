package action

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Del struct {
	keys []string
}

func NewDel(keys []string) *Del {
	return &Del{
		keys: keys,
	}
}

func (q Del) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.keys)+1, builder)
	AppendElement("DEL", builder, false)
	for _, k := range q.keys {
		AppendElement(k, builder, false)
	}
	return nil
}

func (q Del) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case uint64:
		return nil
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
