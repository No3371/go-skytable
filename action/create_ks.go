package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// Note:
// KEYSPACE for Keyspaces
// KEYSPACE:TABLE for tables



type CreateKeyspace struct {
	Path string
}

func FormatSingleCreateKeyspacePacket (path string) string {
	return fmt.Sprintf("*1\n~3\n6\nCREATE\n8\nKEYSPACE\n%d\n%s\n", len(path), path)
}

func (q CreateKeyspace) AppendToPacket(builder *strings.Builder) error {
	if strings.Contains(q.Path, ":") {
		return errors.New("do not include : in the path when creating keyspace")
	}

	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "CREATE", "KEYSPACE", q.Path)
	if err != nil {
		return err
	}
	return nil
}

func (q CreateKeyspace) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespErrStr:
			return nil
		case protocol.RespServerError:
			return nil
		case protocol.RespPacketError: // Always handle this
			return protocol.NewUnexpectedProtocolError("CreateKeyspace: received PacketError, please try to report to go-skytable issues.", nil)
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateKeyspace: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateKeyspace: Unexpected response element: %v", response), nil)
	}
}
