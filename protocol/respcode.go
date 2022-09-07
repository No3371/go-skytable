package protocol

//go:generate stringer -type=ResponseCode
type ResponseCode int8

const (
	RespErr			ResponseCode = -2     // NOTE: A package-customized value not defined in Skyhash. 
	RespErrStr			ResponseCode = -1 // NOTE: A package-customized value not defined in Skyhash. 
	RespOkay            ResponseCode = 0
	RespNil             ResponseCode = 1
	RespOverwriteError  ResponseCode = 2
	RespActionError     ResponseCode = 3
	RespPacketError     ResponseCode = 4
	RespServerError     ResponseCode = 5
	RespOtherError      ResponseCode = 6
	RespWrongtypeError  ResponseCode = 7
	RespUnknownDataType ResponseCode = 8
	RespEncodingError   ResponseCode = 9
	RespBadCredentials  ResponseCode = 10
	RespAuthnRealmError ResponseCode = 11
)

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
