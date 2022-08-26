package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/del
type Del struct {
	Keys []string
}

func (q Del) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Keys)+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement("DEL", builder, false)
	if err != nil {
		return err
	}

	for _, k := range q.Keys {
		err = AppendElement(k, builder, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q Del) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case uint64:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DEL: Unexpected response element: %v", response), nil)
	}
}
