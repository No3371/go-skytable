package action

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Login struct {
	username string
	token    string
}

func NewLogin(username string, token string) *Login {
	return &Login{
		username: username,
		token:    token,
	}
}

func (q Login) AppendToPacket(builder *strings.Builder) error {
	AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
	AppendElement("AUTH", builder, false)
	AppendElement("LOGIN", builder, false)
	AppendElement(q.username, builder, false)
	AppendElement(q.token, builder, false)
	return nil
}

func (q Login) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespBadCredentials:
			return nil
		default:
			return protocol.ErrUnexpectedProtocol
		}
	default:
		return protocol.ErrUnexpectedProtocol
	}
}
