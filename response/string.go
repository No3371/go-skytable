package response

import "io"

func (rr ResponseReader) readStringValue(bytes int64) (string, error) {
	var str []byte = make([]byte, bytes)
	_, err := io.ReadFull(rr.reader, str)
	if err != nil {
		return "", err
	}

	return string(str), nil
}