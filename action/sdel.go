package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/sdel
type SDel struct {
	Keys []string
}

func (q SDel) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Keys)+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement("SDEL", builder, false)
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

func (q SDel) ValidateProtocol(response interface{}) error {
	switch resp := response.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SDEL: Unexpected response code: %v", resp), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SDEL: Unexpected response element: %v", response), nil)
	}
}
