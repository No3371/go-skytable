package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type CreateTable struct {
	Path             string
	ModelDescription any
}

func FormatSingleCreateTablePacket(path string, modelDesc any) (string, error) {
	switch modelDesc := modelDesc.(type) {
	case protocol.KeyMapDescription:
		m := modelDesc.Model()
		if modelDesc.Volatile {
			return fmt.Sprintf("*1\n~5\n6\nCREATE\n%d\n%s\n%d\n%s\n8\nvolatile\n", len(path), path, len(m), m), nil
		} else {
			return fmt.Sprintf("*1\n~4\n6\nCREATE\n%d\n%s\n%d\n%s\n", len(path), path, len(m), m), nil
		}
	default:
		return "", errors.New("unexpected model description")
	}
}

func (q CreateTable) AppendToPacket(builder *strings.Builder) error {
	if !strings.Contains(q.Path, ":") {
		return errors.New("use explicit full path to the table to drop it (keyspace:table)")
	}

	switch modelDesc := q.ModelDescription.(type) {
	case protocol.KeyMapDescription:
		if modelDesc.Volatile {
			AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 5, builder)
		} else {
			AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
		}
		AppendElement("CREATE", builder, false)
		AppendElement("TABLE", builder, false)
		AppendElement(q.Path, builder, false)

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
		case protocol.RespOkay:
			return nil
		case protocol.RespErrStr:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateTable: Unexpected response element: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("CreateTable: Unexpected response element: %v", response), nil)
	}
}
