package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/mpop
type MPop struct {
	Keys []string
}

func (q MPop) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Keys)+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement(builder, false, "MPOP")
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

func (q MPop) ValidateProtocol(response interface{}) error {
	switch resp := response.(type) {
	case *protocol.TypedArray:
		return nil
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MPOP: Unexpected response code: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MPOP: Unexpected response element: %v", response), nil)
	}
}
