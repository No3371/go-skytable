package protocol

import "fmt"

type ErrorStringResponse struct {
	err string
}

func NewServerErrorResponse(err string) *ErrorStringResponse {
	return &ErrorStringResponse{
		err: err,
	}
}

func (r ErrorStringResponse) Error() string {
	return fmt.Sprintf("skytable response error string: %s", r.err)
}