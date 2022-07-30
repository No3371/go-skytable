package response

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/No3371/go-skytable/protocol"
)

var ErrIncompletePacket = errors.New("incomplete packet")
var ErrInvalidPacket = errors.New("invalid packet")
var ErrDataLengthMismatch = errors.New("data length mismatch")
var ErrNotImplementedDataType = errors.New("datatype not implemented")

type ResponseEntry struct {
	DataType protocol.DataType
	Value    any
	Err      error
}

var EmptyResponseEntry ResponseEntry = ResponseEntry{
	protocol.DataTypeUnknown, nil, nil,
}

type ResponseReader struct {
	reader *bufio.Reader
}

func NewResponseReader() *ResponseReader {
	return &ResponseReader{
		reader: bufio.NewReader(nil),
	}
}

func (rr ResponseReader) Read(r io.Reader) ([]ResponseEntry, error) {
	rr.reader.Reset(r)
	count, err := rr.readMetaframe()
	if err != nil {
		return nil, fmt.Errorf("an error occured when reading metaframe: %w", err)
	}

	var entries []ResponseEntry = make([]ResponseEntry, count)

	for i := int64(0); i < count; i++ {
		dt, v, err := rr.readOneEntry()
		if err != nil && !errors.Is(err, ErrNotImplementedDataType) {
			return entries, fmt.Errorf("an error occured when reading entry#%d/%d: %w", i+1, count, err)
		}

		entries[i] = ResponseEntry{
			Value:    v,
			DataType: dt,
			Err:      err,
		}
	}

	return entries, nil
}

func (rr ResponseReader) readMetaframe() (int64, error) {
	read, err := rr.reader.ReadBytes('\n')

	if err != nil {
		return 0, err
	}

	if read[0] != '*' || read[len(read)-1] != '\n' {
		return 0, ErrInvalidPacket
	}

	log.Printf("    metaframe: %v", read)

	length, err := strconv.ParseInt(string(read[1:len(read)-1]), 10, 64)
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (rr ResponseReader) readOneEntry() (dt protocol.DataType, v interface{}, err error) {
	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return 0, nil, err
	}

	length, err := strconv.ParseInt(string(read[1:len(read)-1]), 10, 64)
	if err != nil {
		return 0, nil, err
	}

	dt = protocol.DataType(read[0])

	if DEBUG {
		log.Printf("    type: %c, entry length: %d", dt, length)
	}

	switch dt {
	case protocol.DataTypeString: // string
		v, err = rr.readStringValue(length)
		return dt, v, err
	case protocol.DataTypeResponseCode: // resp code
		v, err = rr.readResponseCode(length)
		return dt, v, err
	case protocol.DataTypeBinaryString: // binary_string
		v, err = rr.readBinaryStringValue(length)
		return dt, v, err
	case protocol.DataTypeJson: // json
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	case protocol.DataTypeSmallint: // uint8
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	case protocol.DataTypeSmallintSigned: // int8
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	case protocol.DataTypeInt: // uint64
		v, err = rr.readUint64(length)
		return dt, v, err
	case protocol.DataTypeIntSigned: // int64
		v, err = rr.readInt64(length)
		return dt, v, err
	case protocol.DataTypeFloat: // float32
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	// arrays
	// case '&': // recursive array
	// case '_': // flat (non-recursive) array
	case '@': // typed array
		v, err = rr.readTypedArray(protocol.SimpleType(read[1]), length)
		return dt, v, err
	// case '~': // any array
	// case '^': // typed non-null array
	default:
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	}
}

func (rr ResponseReader) ReadSimpleType(t protocol.SimpleType, length int64) (interface{}, error) {
	switch t {
	case protocol.SimpleTypeString: // string
		return rr.readStringValue(length)
	case protocol.SimpleTypeResponseCode: // resp code
		return rr.readResponseCode(length)
	case protocol.SimpleTypeBinaryString: // binary_string
		return rr.readBinaryStringValue(length)
	case protocol.SimpleTypeJson: // json
		return nil, ErrNotImplementedDataType
	case protocol.SimpleTypeSmallint: // uint8
		return nil, ErrNotImplementedDataType
	case protocol.SimpleTypeSmallintSigned: // int8
		return nil, ErrNotImplementedDataType
	case protocol.SimpleTypeInt: // uint64
		return rr.readUint64(length)
	case protocol.SimpleTypeIntSigned: // int64
		return rr.readInt64(length)
	case protocol.SimpleTypeFloat: // float32
		return nil, ErrNotImplementedDataType
	default:
		return nil, ErrNotImplementedDataType
	}
}
