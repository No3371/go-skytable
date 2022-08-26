package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/ddl/#use
//
// “USE KEYSPACE” and “USE TABLE” are unified into “USE”.
type Use struct {
	Path string
}

func FormatSingleUsePacket(path string) string {
	return fmt.Sprintf("*1\n~2\n3\nUSE\n%d\n%s\n", len(path), path)
}

func (q Use) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "USE", q.Path)
	if err != nil {
		return err
	}

	return nil
}

func (q Use) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespNil:
			return nil
		case protocol.RespErrStr:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Use: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Use: Unexpected response element: %v", response), nil)
	}
}
