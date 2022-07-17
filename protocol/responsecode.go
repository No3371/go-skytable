package protocol

import "fmt"

//go:generate stringer -type=ResponseCode
type ResponseCode int

const (
	Okay            ResponseCode = 0
	Nil             ResponseCode = 1
	OverwriteError  ResponseCode = 2
	ActionError     ResponseCode = 3
	PacketError     ResponseCode = 4
	ServerError     ResponseCode = 5
	OtherError      ResponseCode = 6
	WrongtypeError  ResponseCode = 7
	UnknownDataType ResponseCode = 8
	EncodingError   ResponseCode = 9
	BadCredentials  ResponseCode = 10
	AuthnRealmError ResponseCode = 11
)

type ServerErrorResponse struct {
	err string
}

func NewServerErrorResponse (err string) *ServerErrorResponse {
	return &ServerErrorResponse{
		err: err,
	}
}

func (r *ServerErrorResponse) Error () string {
	return fmt.Sprintf("skytable response error code: %s", r.err)
}


/*
0	Okay	The server succeded in carrying out some operation
1	Nil	The client asked for a non-existent object
2	Overwrite Error	The client tried to overwrite data
3	Action Error	The action didn't expect the arguments sent
4	Packet Error	The packet contains invalid data
5	Server Error	An error occurred on the server side
6	Other error	Some other error response
7	Wrongtype error	The client sent the wrong type
8	Unknown data type	The client sent an unknown data type
9	Encoding error	The client sent a badly encoded query
10	Bad credentials	The authn credentials are invalid
11	Authn realm error	The current user is not allowed to perform the action
*/