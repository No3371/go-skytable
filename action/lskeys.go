package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/lskeys
type LSKeys struct {
	Entity string // If "", omitted in the sent command
	Limit  uint64 // If 0, omitted in the sent command
}

func (q LSKeys) AppendToPacket(builder *strings.Builder) (err error) {
	if q.Entity != "" && q.Limit != 0 {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "LSKEYS", q.Entity, q.Limit)
		if err != nil {
			return err
		}

		return nil

	} else if q.Entity != "" {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "LSKEYS", q.Entity)
		if err != nil {
			return err
		}

		return nil
	} else if q.Limit != 0 {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "LSKEYS", q.Limit)
		if err != nil {
			return err
		}

		return nil
	} else {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 1, builder)
		if err != nil {
			return err
		}

		err = AppendElement(builder, false, "LSKEYS")
		if err != nil {
			return err
		}

		return nil
	}
}

func (q LSKeys) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case *protocol.TypedArray:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LSKEYS: Unexpected response element: %v", response), nil)
	}
}
