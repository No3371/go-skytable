package protocol

import "errors"

var ErrProtocolVersion = errors.New("connected Skytable instance implement different protocol version")
var ErrUnexpectedProtocol = errors.New("does not match implemented protocol")
var ErrIncorrectArrayUsage = errors.New("wrong usage of array structs")
var ErrWrongDataType = errors.New("recorded type does not match getting type")