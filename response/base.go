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
var ErrElementSizeMismatch = errors.New("element size mismatch")
var ErrNotImplementedDataType = errors.New("datatype not implemented")

type ResponseEntry struct {
	DataType protocol.DataType
	Value    any
	Err      error
}

var EmptyResponseEntry ResponseEntry = ResponseEntry{
	protocol.DataTypeUnknown, nil, nil,
}

type ResponseEntryTypedWrapper[T any] struct {
	ResponseEntry
}

func (w ResponseEntryTypedWrapper[T]) Get() (T, error) {
	t, casted := w.ResponseEntry.Value.(T)
	if !casted {
		return t, errors.New("wrapped value can't be casted to the type")
	} else {
		return t, nil
	}
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
		if err != nil {
			var errErrStr protocol.ErrorStringResponse
			if ! (dt == protocol.DataTypeResponseCode && !errors.As(err, &errErrStr)) { // error strings are passed into the entry
				return entries, fmt.Errorf("an error occured when reading entry#%d/%d: %w", i+1, count, err)
			}
		}

		if dt == protocol.DataTypeResponseCode && v == protocol.RespPacketError {
			return nil, protocol.ErrCodePacketError
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

	if DEBUG {
		log.Printf("    metaframe: %v", read)
	}

	length, err := strconv.ParseInt(string(read[1:len(read)-1]), 10, 64)
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (rr ResponseReader) readOneEntry() (dt protocol.DataType, v interface{}, err error) {
	tByte, err := rr.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return 0, nil, err
	}

	dt = protocol.DataType(tByte)
	size := int64(0)

	switch dt {
	case protocol.DataTypeString: // string
		fallthrough
	case protocol.DataTypeResponseCode: // resp code
		fallthrough
	case protocol.DataTypeBinaryString: // binary_string
		fallthrough
	case protocol.DataTypeJson: // json
		fallthrough
	case protocol.DataTypeSmallint: // uint8
		fallthrough
	case protocol.DataTypeSmallintSigned: // int8
		fallthrough
	case protocol.DataTypeInt: // uint64
		fallthrough
	case protocol.DataTypeIntSigned: // int64
		fallthrough
	case protocol.DataTypeFloat: // float32
		fallthrough
	// arrays
	// case '&': // recursive array
	case protocol.DataTypeFlatArray: // flat (non-recursive) array
		size, err = strconv.ParseInt(string(read[:len(read)-1]), 10, 64)
		if err != nil {
			return 0, nil, err
		}
	// case '~': // any array
	case protocol.DataTypeTypedArray: // typed array
		fallthrough
	case protocol.DataTypeTypedNonNullArray: // typed non-null array
		size, err = strconv.ParseInt(string(read[1:len(read)-1]), 10, 64)
		if err != nil {
			return 0, nil, err
		}
	default:
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	}

	if DEBUG {
		log.Printf("    read type: %c, entry size: %d", dt, size)
	}

	switch dt {
	case protocol.DataTypeString: // string
		v, err = rr.readStringValue(size)
		return dt, v, err
	case protocol.DataTypeResponseCode: // resp code
		v, err = rr.readResponseCode(size)
		return dt, v, err
	case protocol.DataTypeBinaryString: // binary_string
		v, err = rr.readBinaryStringValue(size)
		return dt, v, err
	case protocol.DataTypeJson: // json
		return dt, v, ErrNotImplementedDataType
	case protocol.DataTypeSmallint: // uint8
		return dt, v, ErrNotImplementedDataType
	case protocol.DataTypeSmallintSigned: // int8
		return dt, v, ErrNotImplementedDataType
	case protocol.DataTypeInt: // uint64
		v, err = rr.readUint64(size)
		return dt, v, err
	case protocol.DataTypeIntSigned: // int64
		v, err = rr.readInt64(size)
		return dt, v, err
	case protocol.DataTypeFloat: // float32
		v, err = rr.readFloat32(size)
		return dt, v, err
	// arrays
	case protocol.DataTypeArray: // recursive
		return dt, v, ErrNotImplementedDataType
	case protocol.DataTypeFlatArray:
		v, err = rr.readFlatArray(size)
		return dt, v, err
	case protocol.DataTypeAnyArray:
		return dt, v, ErrNotImplementedDataType
	case protocol.DataTypeTypedArray:
		v, err = rr.readTypedArray(protocol.SimpleType(read[0]), size)
		return dt, v, err
	case protocol.DataTypeTypedNonNullArray:
		v, err = rr.readTypedNonNullArray(protocol.SimpleType(read[0]), size)
		return dt, v, err
	default:
		v, err = nil, ErrNotImplementedDataType
		return dt, v, err
	}
}

func (rr ResponseReader) readOneEntryTyped(dt protocol.DataType) (v interface{}, err error) {

	read, err := rr.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if DEBUG {
		log.Printf("    typed read: %v", read)
	}

	if read[0] == 0 && read[1] == '0' { // NULL
		return nil, nil
	}

	length, err := strconv.ParseInt(string(read[:len(read)-1]), 10, 64)
	if err != nil {
		return nil, err
	}

	if DEBUG {
		log.Printf("    typed: %c, entry size: %d", dt, length)
	}

	switch dt {
	case protocol.DataTypeString: // string
		v, err = rr.readStringValue(length)
		return v, err
	case protocol.DataTypeResponseCode: // resp code
		v, err = rr.readResponseCode(length)
		return v, err
	case protocol.DataTypeBinaryString: // binary_string
		v, err = rr.readBinaryStringValue(length)
		return v, err
	case protocol.DataTypeJson: // json
		return v, ErrNotImplementedDataType
	case protocol.DataTypeSmallint: // uint8
		return v, ErrNotImplementedDataType
	case protocol.DataTypeSmallintSigned: // int8
		return v, ErrNotImplementedDataType
	case protocol.DataTypeInt: // uint64
		v, err = rr.readUint64(length)
		return v, err
	case protocol.DataTypeIntSigned: // int64
		v, err = rr.readInt64(length)
		return v, err
	case protocol.DataTypeFloat: // float32
		v, err = rr.readFloat32(length)
		return v, err
	// arrays
	case protocol.DataTypeArray: // recursive
		return v, ErrNotImplementedDataType
	case protocol.DataTypeFlatArray:
		v, err = rr.readFlatArray(length)
		return v, err
	case protocol.DataTypeAnyArray:
		return v, ErrNotImplementedDataType
	case protocol.DataTypeTypedArray:
		v, err = rr.readTypedArray(protocol.SimpleType(read[0]), length)
		return v, err
	case protocol.DataTypeTypedNonNullArray:
		v, err = rr.readTypedNonNullArray(protocol.SimpleType(read[0]), length)
		return v, err
	default:
		v, err = nil, ErrNotImplementedDataType
		return v, err
	}
}

func (rr ResponseReader) ReadSimpleType(t protocol.SimpleType, size int64) (interface{}, error) {
	switch t {
	case protocol.SimpleTypeString: // string
		return rr.readStringValue(size)
	case protocol.SimpleTypeResponseCode: // resp code
		return rr.readResponseCode(size)
	case protocol.SimpleTypeBinaryString: // binary_string
		return rr.readBinaryStringValue(size)
	case protocol.SimpleTypeJson: // json
		return nil, ErrNotImplementedDataType
	case protocol.SimpleTypeSmallint: // uint8
		return nil, ErrNotImplementedDataType
	case protocol.SimpleTypeSmallintSigned: // int8
		return nil, ErrNotImplementedDataType
	case protocol.SimpleTypeInt: // uint64
		return rr.readUint64(size)
	case protocol.SimpleTypeIntSigned: // int64
		return rr.readInt64(size)
	case protocol.SimpleTypeFloat: // float32
		return rr.readFloat32(size)
	default:
		return nil, ErrNotImplementedDataType
	}
}
