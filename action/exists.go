package action

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Exists struct {
	keys []string
}

func NewExists(keys []string) *Exists {
	return &Exists{
		keys: keys,
	}
}

func (q Exists) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.keys)+1, builder)
	AppendElement("EXISTS", builder, false)
	for _, k := range q.keys {
		AppendElement(k, builder, false)
	}
	return nil
}

func (q Exists) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case uint64:
		return nil
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
