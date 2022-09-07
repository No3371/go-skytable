package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/lmod#push
type LModPush struct {
	ListName   string
	Elements []any
}

func (q LModPush) AppendToPacket(builder *strings.Builder) (err error) {
	if q.Elements == nil {
		return errors.New("elements should not be nil")
	} else {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3 + len(q.Elements), builder)
	}

	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LMOD", q.ListName, "PUSH")
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, q.Elements...)
	if err != nil {
		return err
	}
	return nil
}

func (q LModPush) ValidateProtocol(response any) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODPUSH: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODPUSH: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lmod#insert
type LModInsert struct {
	ListName   string
	Index uint64
	Element any
}

func (q LModInsert) AppendToPacket(builder *strings.Builder) (err error) {
	if q.Element == nil {
		return errors.New("element should not be nil")
	} else {
		err = AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 5, builder)
	}

	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LMOD", q.ListName, "INSERT", q.Index, q.Element)
	if err != nil {
		return err
	}

	return nil
}

func (q LModInsert) ValidateProtocol(response any) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODINSERT: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODINSERT: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lmod#pop
type LModPop struct {
	ListName string
}

func (q LModPop) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LMOD", q.ListName, "POP")
	if err != nil {
		return err
	}

	return nil
}

func (q LModPop) ValidateProtocol(response interface{}) error {
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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODPOP: Unexpected response code: %v", response), nil)
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODPOP: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lmod#pop
type LModPopIndex struct {
	ListName string
	Index    uint64
}

func (q LModPopIndex) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LMOD", q.ListName, "POP", q.Index)
	if err != nil {
		return err
	}

	return nil
}

func (q LModPopIndex) ValidateProtocol(response interface{}) error {
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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODPOPINDEX: Unexpected response code: %v", response), nil)
		}
	case string:
		return nil
	case []byte:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODPOPINDEX: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lmod#remove
type LModRemove struct {
	ListName string
	Index    uint64
}

func (q LModRemove) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LMOD", q.ListName, "REMOVE", q.Index)
	if err != nil {
		return err
	}

	return nil
}

func (q LModRemove) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODREMOVE: Unexpected response code: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODREMOVE: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/lmod#clear
type LModClear struct {
	ListName string
}

func (q LModClear) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "LMOD", q.ListName, "CLEAR")
	if err != nil {
		return err
	}

	return nil
}

func (q LModClear) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODCLEAR: Unexpected response code: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("LMODCLEAR: Unexpected response element: %v", response), nil)
	}
}