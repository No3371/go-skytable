package protocol

import "fmt"

type ErrorStringResponse struct {
	Errstr string
}

func NewErrorStringResponse(err string) *ErrorStringResponse {
	return &ErrorStringResponse{
		Errstr: err,
	}
}

func (r ErrorStringResponse) Error() string {
	return fmt.Sprintf("skytable response error string: %s", r.Errstr)
}

const (
	ErrStr_ContainerNotFound string = "container-not-found"
)