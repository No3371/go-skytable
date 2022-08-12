package action

import (
	"fmt"
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
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "LOGIN", q.username, q.token)
	if err != nil {
		return err
	}
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
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGIN: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGIN: Unexpected response element: %v", response), nil)
	}
}
