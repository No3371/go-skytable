package protocol

import "fmt"

type ErrorCodeResponse struct {
	code ResponseCode
}

func (e ErrorCodeResponse) Error() string {
	return fmt.Sprintf("skytable response error code: %v", e.code)
}

var ErrCodeNil *ErrorCodeResponse = &ErrorCodeResponse{RespNil}
var ErrCodeOverwriteError *ErrorCodeResponse = &ErrorCodeResponse{RespOverwriteError}
var ErrCodeActionError *ErrorCodeResponse = &ErrorCodeResponse{RespActionError}
var ErrCodePacketError *ErrorCodeResponse = &ErrorCodeResponse{RespPacketError}
var ErrCodeServerError *ErrorCodeResponse = &ErrorCodeResponse{RespServerError}
var ErrCodeOtherError *ErrorCodeResponse = &ErrorCodeResponse{RespOtherError}
var ErrCodeWrongtypeError *ErrorCodeResponse = &ErrorCodeResponse{RespWrongtypeError}
var ErrCodeUnknownDataType *ErrorCodeResponse = &ErrorCodeResponse{RespUnknownDataType}
var ErrCodeEncodingError *ErrorCodeResponse = &ErrorCodeResponse{RespEncodingError}
var ErrCodeBadCredentials *ErrorCodeResponse = &ErrorCodeResponse{RespBadCredentials}
var ErrCodeAuthnRealmError *ErrorCodeResponse = &ErrorCodeResponse{RespAuthnRealmError}
