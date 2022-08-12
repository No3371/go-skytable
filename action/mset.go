package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type MSetA struct {
	entries []MSetAEntry
}

type MSetAEntry struct {
	k string
	v any
}


func NewMSetA(entries []MSetAEntry) *MSetA {
	return &MSetA{
		entries: entries,
	}
}


func (q MSetA) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.entries) * 2 + 1, builder)
	AppendElement("MSET", builder, false)
	for _, e := range q.entries {
		AppendElement(e.k, builder, false)
		AppendElement(e.v, builder, false)
	}
	return nil
}

func (q MSetA) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case uint64:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSET: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSET: Unexpected response element: %v", response), nil)
	}
}

type MSetB struct {
	keys []string
	values []any
}

func NewMSetB(keys []string, values []any) *MSetB {
	return &MSetB {
		keys: keys,
		values: values,
	}
}

func (q MSetB) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.keys) * 2 + 1, builder)
	AppendElement("MSET", builder, false)
	for i := range q.keys {
		AppendElement(q.keys[i], builder, false)
		AppendElement(q.values[i], builder, false)
	}
	return nil
}

func (q MSetB) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case uint64:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSET: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MSET: Unexpected response element: %v", response), nil)
	}
}