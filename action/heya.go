package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Heya struct {
	echo string
}

func NewHeya(echo string) *Heya {
	return &Heya{
		echo: echo,
	}
}

func (q Heya) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElement("HEYA", builder, false)
	if err != nil {
		return err
	}

	if q.echo != "" {
		err = AppendElement(q.echo, builder, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q Heya) ValidateProtocol(response interface{}) error {
	switch echo := response.(type) {
	case string:
		if (q.echo == "" && echo != "HEY!") || echo != q.echo {
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("HEYA: unexpected echo: %s", response), nil)
		} else {
			return nil
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("HEYA: Unexpected response element: %v", response), nil)
	}
}
