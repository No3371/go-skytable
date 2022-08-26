package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/ddl/#tables-1
type DropTable struct {
	Path string
}

func FormatSingleDropTablePacket (path string) string {
	return fmt.Sprintf("*1\n~3\n4\nDROP\n5\nTABLE\n%d\n%s\n", len(path), path)
}

func (q DropTable) AppendToPacket(builder *strings.Builder) error {
	if !strings.Contains(q.Path, ":") {
		return errors.New("use explicit full path to the table to drop it (keyspace:table)")
	}

	_, err := fmt.Fprintf(builder, "~3\n4\nDROP\n5\nTABLE\n%d\n%s\n", len(q.Path), q.Path)
	return err
}

func (q DropTable) ValidateProtocol(response interface{}) error {
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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropTable: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("DropTable: Unexpected response element: %v", response), nil)
	}
}
