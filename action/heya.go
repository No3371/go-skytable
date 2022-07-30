package action

import (
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
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	AppendElement("HEYA", builder, false)
	if q.echo != "" {
		AppendElement(q.echo, builder, false)
	}
	return nil
}

func (q Heya) ValidateProtocol(response interface{}) error {
	switch echo := response.(type) {
	case string:
		if (q.echo == "" && echo != "HEY!") || echo != q.echo {
			return protocol.ErrUnexpectedProtocol
		} else {
			return nil
		}
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
