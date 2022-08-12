package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type DropKeyspace struct {
	Path string
}

// ⚠️ Only use this when sending packets contains this action only.
func FormatSingleDropKeyspacePacket (path string) string {
	return fmt.Sprintf("*1\n~3\n4\nDROP\n8\nKEYSPACE\n%d\n%s\n", len(path), path)
}

func (q DropKeyspace) AppendToPacket(builder *strings.Builder) error {
	if strings.Contains(q.Path, ":") {
		return errors.New("do not include : in the path when dropping keyspace")
	}

	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "DROP", "KEYSPACE", q.Path)
	if err != nil {
		return err
	}
	return nil
}

func (q DropKeyspace) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespErrStr:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropKeyspace: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropKeyspace: Unexpected response element: %v", response), nil)
	}
}
