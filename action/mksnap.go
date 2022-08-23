package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// If Path is left empty, "INSPECT KEYSPACE" will be sent (inspect current keyspace)
type MKSnap struct {
	Name string
}

func FormatSingleMKSnapPacket(name string) string {
	if name == "" {
		return "*1\n~1\n6\nMKSNAP\n"
	} else {
		return fmt.Sprintf("*1\n~2\n6\nMKSNAP\n%d\n%s\n", len(name), name)
	}
}

func (q MKSnap) AppendToPacket(builder *strings.Builder) error {
	if q.Name == "" {
		_, err := builder.WriteString("~1\n6\nMKSNAP\n")
		if err != nil {
			return err
		}
	}

	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "MKSNAP", q.Name)
	if err != nil {
		return err
	}

	return nil
}

func (q MKSnap) ValidateProtocol(response interface{}) error {
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
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MKSnap: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("MKSnap: Unexpected response element: %v", response), nil)
	}
}
