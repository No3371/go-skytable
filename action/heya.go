package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/heya
type Heya struct {
	Echo string
}

func (q Heya) AppendToPacket(builder *strings.Builder) (err error) {
	if q.Echo == "" {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 1, builder)
	} else {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	}
	if err != nil {
		return err
	}

	err = AppendElement("HEYA", builder, false)
	if err != nil {
		return err
	}

	if q.Echo != "" {
		err = AppendElement(q.Echo, builder, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q Heya) ValidateProtocol(response interface{}) error {
	switch echo := response.(type) {
	case string:
		if (q.Echo == "" && echo != "HEY!") || (q.Echo != "" && echo != q.Echo) {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("HEYA: unexpected echo: %s", response), nil)
		} else {
			return nil
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("HEYA: Unexpected response element: %v", response), nil)
	}
}
