package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/ddl/#keyspaces-1
type DropKeyspace struct {
	Name string
}

// ⚠️ Only use this when sending packets contains this action only.
func FormatSingleDropKeyspacePacket(path string) string {
	return fmt.Sprintf("*1\n~3\n4\nDROP\n8\nKEYSPACE\n%d\n%s\n", len(path), path)
}

func (q DropKeyspace) AppendToPacket(builder *strings.Builder) error {
	if strings.Contains(q.Name, ":") {
		return errors.New("do not include : in the path when dropping keyspace")
	}

	_, err := fmt.Fprintf(builder, "~3\n4\nDROP\n8\nKEYSPACE\n%d\n%s\n", len(q.Name), q.Name)
	return err
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
