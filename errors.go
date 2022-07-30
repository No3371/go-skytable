package skytable

import (
	"fmt"
)

type ErrLocal struct {
	innerErr error
	msg      string
}

func NewLocalError(msg string, err error) ErrLocal {
	return ErrLocal{
		err,
		msg,
	}
}

func (err ErrLocal) Error() string {
	return fmt.Sprintf("%s: %s", err.msg, err.innerErr)
}

func (err ErrLocal) Unwrap() error {
	return err.innerErr
}

type ErrComu struct {
	innerErr error
	msg      string
}

func NewComuError(msg string, err error) ErrComu {
	return ErrComu{
		err,
		msg,
	}
}

func (err ErrComu) Error() string {
	return fmt.Sprintf("%s: %s", err.msg, err.innerErr)
}

func (err ErrComu) Unwrap() error {
	return err.innerErr
}

type ErrInvalidUsage ErrLocal

func NewUsageError(msg string, err error) ErrInvalidUsage {
	return ErrInvalidUsage{
		err,
		msg,
	}
}

func (err ErrInvalidUsage) Error() string {
	if err.innerErr != nil {
		return fmt.Sprintf("%s: %s", err.msg, err.innerErr)
	} else {
		return err.msg
	}
}

func (err ErrInvalidUsage) Unwrap() error {
	return err.innerErr
}