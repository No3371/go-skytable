package query

import (
	"log"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Set struct {
	key string
	value interface{}
}

func NewSet (key string, value interface{}) *Set {
    return &Set{
		key: key,
		value: value,
    }
}

func (q *Set) AppendToPacket(builder *strings.Builder) error {
    AppendArrayHeader(protocol.ArrayTypeAnyArray, 0, 3, builder)
	AppendValue("SET", builder, false)
	AppendValue(q.key, builder, false)
    log.Printf("sending key length: %d", len(q.key))
	AppendValue(q.value, builder, false)
    log.Printf("sending value length: %d (string)", len(q.value.(string)))
    log.Printf("sending value length: %d (bytes)", len([]byte(q.value.(string))))
    return nil
}