package query

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Get struct {
	key string
}

func NewGet (key string) *Get {
    return &Get{
        key: key,
    }
}

func (q Get) AppendToPacket(builder *strings.Builder) error {
    AppendArrayHeader(protocol.ArrayTypeAnyArray, 0, 2, builder)
	AppendValue("GET", builder, false)
	AppendValue(q.key, builder, false)
    return nil
}