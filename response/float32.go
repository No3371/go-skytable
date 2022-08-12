package response

import "strconv"

func (rr ResponseReader) readFloat32(chars int64) (float32, error) {
	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return 0, err
	}

	if len(read) != int(chars+1) {
		return 0, ErrElementSizeMismatch
	}

	f, err := strconv.ParseFloat(string(read[:len(read)-1]), 32)
	if err != nil {
		return 0, err
	}

	return float32(f), nil
}
