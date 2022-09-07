package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/lset
type LSet struct {
	ListName   string
	Elements []any
}

func (q LSet) AppendToPacket(builder *strings.Builder) (err error) {
	if q.Elements == nil {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	} else {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2 + len(q.Elements), builder)
	}

	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LSET", q.ListName)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, q.Elements...)
	if err != nil {
		return err
	}
	return nil
}

func (q LSet) ValidateProtocol(response any) error {
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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LSET: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LSET: Unexpected response element: %v", response), nil)
	}
}
