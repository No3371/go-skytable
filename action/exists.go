package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/exists
type Exists struct {
	Keys []string
}

func (q Exists) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Keys)+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement("EXISTS", builder, false)
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

func (q Exists) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case uint64:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("EXISTS: Unexpected response element: %v", response), nil)
	}
}
