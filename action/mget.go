package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/mget
type MGet struct {
	Keys []string
}

func (q MGet) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Keys)+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement(builder, false, "MGET")
	if err != nil {
		return err
	}

	for _, k := range q.Keys {
		err = AppendElement(builder, false, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q MGet) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case *protocol.TypedArray:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MGET: Unexpected response element: %v", response), nil)
	}
}
