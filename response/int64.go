package response

import "strconv"

func (rr ResponseReader) readInt64(chars int64) (int64, error) {
	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return 0, err
	}

	if len(read) != int(chars+1) {
		return 0, ErrDataLengthMismatch
	}

	return strconv.ParseInt(string(read[:len(read)-1]), 10, 64)
}