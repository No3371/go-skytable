package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/uset
type USet struct {
	Entries []KVPair
}

func (q USet) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Entries)*2+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement(builder, false, "USET")
	if err != nil {
		return err
	}

	for _, p := range q.Entries {
		err = AppendElements(builder, false, p.K, p.V)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q USet) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case uint64:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("USET: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("USET: Unexpected response element: %v", response), nil)
	}
}
