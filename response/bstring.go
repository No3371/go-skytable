package response

import "io"

func (rr ResponseReader) readBinaryStringValue(bytes int64) ([]byte, error) {
	var str []byte = make([]byte, bytes)
	_, err := io.ReadFull(rr.reader, str)
	if err != nil {
		return nil, err
	}

	rr.reader.ReadByte() // discard trailing \n

	return str, nil
}