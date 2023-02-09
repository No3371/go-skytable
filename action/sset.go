package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/sset
type SSet struct {
	Entries []KVPair
}

func (q SSet) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Entries)*2+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement(builder, false, "SSET")
	if err != nil {
		return err
	}

	for _, e := range q.Entries {
		err = AppendElement(builder, false, e.K)
		if err != nil {
			return err
		}

		err = AppendElement(builder, false, e.V)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q SSet) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespOverwriteError:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SSET: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("SSET: Unexpected response element: %v", response), nil)
	}
}
