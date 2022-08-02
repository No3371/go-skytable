package protocol

import (
	"errors"
	"fmt"
)

var ErrProtocolVersion = errors.New("connected Skytable instance implement different protocol version")
var ErrIncorrectArrayUsage = errors.New("wrong usage of array structs")
var ErrWrongDataType = errors.New("recorded type does not match getting type")


type ErrUnexpectedProtocol struct {
    innerErr error
    msg string
}

func NewUnexpectedProtocolError(msg string, err error) ErrUnexpectedProtocol {
	return ErrUnexpectedProtocol{
		err,
		msg,
	}
}

func (err ErrUnexpectedProtocol) Error() string {
	if err.innerErr != nil {
		return fmt.Sprintf("unexpected protocol: %s: %s", err.msg, err.innerErr)
	} else {
		return err.msg
	}
}

func (err ErrUnexpectedProtocol) Unwrap() error {
	return err.innerErr
}