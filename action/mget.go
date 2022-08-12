package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type MGet struct {
	keys []string
}

func NewMGet(keys []string) *MGet {
	return &MGet{
		keys: keys,
	}
}

func (q MGet) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.keys) + 1, builder)
	if err != nil {
		return err
	}

	err = AppendElement("MGET", builder, false)
	if err != nil {
		return err
	}

	for _, k := range q.keys {
		err = AppendElement(k, builder, false)
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
