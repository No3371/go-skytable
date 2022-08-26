package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/keylen
type KeyLen struct {
	Key string
}

func (q KeyLen) AppendToPacket(builder *strings.Builder) error {
	if q.Key == "" {
		return errors.New("KeyLen: empty key")
	}

	_, err := fmt.Fprintf(builder, "~2\n6\nKEYLEN\n%d\n%s\n", len(q.Key), q.Key)
	return err
}

func (q KeyLen) ValidateProtocol(response interface{}) error {
	switch response.(type) {
	case uint64:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.ErrCodeNil:
			return nil
		case protocol.ErrCodeServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("KeyLen: Unexpected response code: %v", response), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("KeyLen: Unexpected response element: %v", response), nil)
	}
}
