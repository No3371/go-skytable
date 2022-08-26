package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/mset
type MSetA struct {
	Entries []KVPair
}

func (q MSetA) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Entries)*2+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement("MSET", builder, false)
	if err != nil {
		return err
	}

	for _, e := range q.Entries {
		err = AppendElement(e.K, builder, false)
		if err != nil {
			return err
		}

		err = AppendElement(e.V, builder, false)
		if err != nil {
			return err
		}
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

// https://docs.skytable.io/actions/mset
type MSetB struct {
	Keys   []string
	Values []any
}

func (q MSetB) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Keys)*2+1, builder)
	AppendElement("MSET", builder, false)
	for i := range q.Keys {
		AppendElement(q.Keys[i], builder, false)
		AppendElement(q.Values[i], builder, false)
	}
	return nil
}

func (q MSetB) ValidateProtocol(response any) error {
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
