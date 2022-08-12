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

func NewDropKeyspace(path string) *DropKeyspace {
	return &DropKeyspace{
		Path: path,
	}
}

func FormatDropKeyspace (path string) string {
	return fmt.Sprintf("*1\n~3\n4\nDROP\n8\nKEYSPACE\n%d\n%s\n", len(path), path)
}

func (q DropKeyspace) AppendToPacket(builder *strings.Builder) error {
	if strings.Contains(q.Path, ":") {
		return errors.New("do not include : in the path when dropping keyspace")
	}

	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	AppendElement("DROP", builder, false)
	AppendElement("KEYSPACE", builder, false)
	AppendElement(q.Path, builder, false)
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
