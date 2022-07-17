package query

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Del struct {
	keys []string
}

func NewDel (keys []string) *Del {
    return &Del{
        keys: keys,
    }
}

func (q Del) AppendToPacket(builder *strings.Builder) error {
    AppendArrayHeader(protocol.ArrayTypeAnyArray, 0, len(q.keys) + 1, builder)
	AppendValue("DEL", builder, false)
    for _, k := range q.keys {
	    AppendValue(k, builder, false)
    }
    return nil
}