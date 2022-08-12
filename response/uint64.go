package response

import "strconv"

func (rr ResponseReader) readUint64(chars int64) (uint64, error) {
	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return 0, err
	}

	if len(read) != int(chars+1) {
		return 0, ErrElementSizeMismatch
	}

	return strconv.ParseUint(string(read[:len(read)-1]), 10, 64)
}
