package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/mupdate
type MUpdate struct {
	Entries []KVPair
}

func (q MUpdate) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, len(q.Entries)*2+1, builder)
	if err != nil {
		return err
	}

	err = AppendElement("MUPDATE", builder, false)
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

func (q MUpdate) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case uint64:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MUpdate: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MUpdate: Unexpected response element: %v", response), nil)
	}
}