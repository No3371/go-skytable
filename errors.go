package skytable

import (
	"errors"
	"fmt"
    "github.com/No3371/go-skytable/protocol"
)

var ErrUnexpectedProtocol = errors.New("unexepected Skyhash protocol")

type ResponseErrorCode struct {
	code protocol.ResponseCode
}

func (e *ResponseErrorCode) Error() string {
	return fmt.Sprintf("error code: %v", e.code)
}