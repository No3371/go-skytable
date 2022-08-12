package response

import (
	"strconv"

	"github.com/No3371/go-skytable/protocol"
)

func (rr ResponseReader) readResponseCode(chars int64) (protocol.ResponseCode, error) {
	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return -1, err
	}

	if len(read) != int(chars+1) {
		return -1, ErrElementSizeMismatch
	}

	if read[0] >= '0' && read[0] <= '9' {
		i, err := strconv.ParseInt(string(read[:len(read)-1]), 10, 64)
		if err != nil {
			return protocol.RespErr, err
		}

		return protocol.ResponseCode(i), nil
	} else {
		return protocol.RespErrStr, protocol.NewErrorStringResponse(string(read[:len(read)-1]))
	}
}
