package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type DropTable struct {
	Path string
}

func NewDropTable(path string) *DropTable {
	return &DropTable{
		Path: path,
	}
}

func FormatSingleDropTablePacket (path string) string {
	return fmt.Sprintf("*1\n~3\n4\nDROP\n5\nTABLE\n%d\n%s\n", len(path), path)
}

func (q DropTable) AppendToPacket(builder *strings.Builder) error {
	if !strings.Contains(q.Path, ":") {
		return errors.New("use explicit full path to the table to drop it (keyspace:table)")
	}

	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "DROP", "TABLE", q.Path)
	if err != nil {
		return err
	}

	return nil
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
