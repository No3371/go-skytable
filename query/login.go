package query

import (
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

type Login struct {
    username string
    token string
}

func NewLogin (username string, token string) *Login {
    return &Login{
        username: username,
        token: token,
    }
}

func (q Login) AppendToPacket(builder *strings.Builder) error {
    AppendArrayHeader(protocol.ArrayTypeAnyArray, 0, 4, builder)
	AppendValue("AUTH", builder, false)
	AppendValue("LOGIN", builder, false)
	AppendValue(q.username, builder, false)
	AppendValue(q.token, builder, false)
    return nil
}