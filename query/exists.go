package query

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Exists struct {
	keys []string
}

func NewExists (keys []string) *Exists {
    return &Exists{
        keys: keys,
    }
}

func (q Exists) AppendToPacket(builder *strings.Builder) error {
    AppendArrayHeader(protocol.ArrayTypeAnyArray, 0, len(q.keys) + 1, builder)
	AppendValue("EXISTS", builder, false)
    for _, k := range q.keys {
	    AppendValue(k, builder, false)
    }
    return nil
}