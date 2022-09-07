package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/lget#lget
type LGet struct {
	ListName string
	Limit    uint64 // If 0, omitted in the sent command
}

func (q LGet) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LGET", q.ListName)
	if err != nil {
		return err
	}

	if q.Limit != 0 {
		err = AppendElements(builder, false, "LIMIT", q.Limit)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q LGet) ValidateProtocol(response interface{}) error {
	switch resp := response.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespNil:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGET: Unexpected response code: %v", response), nil)
		}
	case *protocol.TypedArray:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGET: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lget#len
type LGetLen struct {
	ListName string
}

func (q LGetLen) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LGET", q.ListName, "LEN")
	if err != nil {
		return err
	}
	return nil
}

func (q LGetLen) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case uint64:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETLEN: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lget#valueat
type LGetValueAt struct {
	ListName string
	Index    uint64
}

func (q LGetValueAt) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LGET", q.ListName, "VALUEAT", q.Index)
	if err != nil {
		return err
	}

	return nil
}

func (q LGetValueAt) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETVALUEAT: Unexpected response code: %v", response), nil)
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETVALUEAT: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lget#first
type LGetFirst struct {
	ListName string
}

func (q LGetFirst) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LGET", q.ListName, "FIRST")
	if err != nil {
		return err
	}

	return nil
}

func (q LGetFirst) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETFIRST: Unexpected response code: %v", response), nil)
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETFIRST: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lget#last
type LGetLast struct {
	ListName string
}

func (q LGetLast) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LGET", q.ListName, "LAST")
	if err != nil {
		return err
	}

	return nil
}

func (q LGetLast) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETLAST: Unexpected response code: %v", response), nil)
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETLAST: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lget#range
type LGetRange struct {
	ListName string
	From     uint64
	To       uint64 // If 0, omitted in the sent command
}

func (q LGetRange) AppendToPacket(builder *strings.Builder) error {
	if q.To != 0 {
		err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 5, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "LGET", q.ListName, "RANGE", q.From, q.To)
		if err != nil {
			return err
		}
	} else {
		err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "LGET", q.ListName, "RANGE", q.From)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q LGetRange) ValidateProtocol(response interface{}) error {
	switch resp := response.(type) {
	case protocol.ResponseCode:
		switch resp {
		case protocol.RespNil:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETRANGE: Unexpected response code: %v", response), nil)
		}
	case *protocol.TypedArray:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LGETRANGE: Unexpected response element: %v", response), nil)
	}
}
