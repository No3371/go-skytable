package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)


type CreateTable struct {
	Path string
	ModelDescription any
}

func FormatCreateTable (path string, modelDesc any) (string, error) {
	switch modelDesc := modelDesc.(type) {
		case protocol.KeyMapDescription:
			m := modelDesc.Model()
			if modelDesc.Volatile {
				return fmt.Sprintf("*1\n~2\n4\nCREATE\n%d\n%s\n%d\n%s\n8\nvolatile\n", len(path), path, len(m), m), nil
			} else {
				return fmt.Sprintf("*1\n~2\n3\nCREATE\n%d\n%s\n%d\n%s\n", len(path), path, len(m), m), nil
			}
		default:
			return "", errors.New("unexpected model description")
	}
}

func (q CreateTable) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	AppendElement("CREATE", builder, false)
	AppendElement("TABLE", builder, false)
	AppendElement(q.Path, builder, false)

	switch modelDesc := q.ModelDescription.(type) {
		case protocol.KeyMapDescription:
			AppendElement(modelDesc.Model(), builder, false)
			if modelDesc.Volatile {
				AppendElement("volatile", builder, false)
			}
		default:
			return errors.New("unexpected model description")
	}

	return nil
}

func (q CreateTable) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespNil:
			return nil
		case protocol.RespErrStr:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateTable: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateTable: Unexpected response element: %v", response), nil)
	}
}
