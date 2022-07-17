package protocol

import "errors"

var ErrUnexpectedProtocol = errors.New("does not match Skyhash protocol")

var ErrIncorrectArrayUsage = errors.New("please review array usage")