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

type ResponseReader struct {
    reader *bufio.Reader
}

func NewResponseReader () *ResponseReader {
    return &ResponseReader{
        reader: bufio.NewReader(nil),
    }
}

func (rr ResponseReader) Read (r io.Reader) ([]interface{}, error) {
    rr.reader.Reset(r)
    count, err := rr.readMetaframe()
    if err != nil {
        return nil, fmt.Errorf("an error occured when reading metaframe: %w", err)
    }

    var entries []interface{} = make([]interface{}, count)

    for i := int64(0); i < count; i++ {
        entries[i], err = rr.readOneEntry()
        if err != nil {
            return entries, fmt.Errorf("an error occured when reading entry#%d/%d: %w", i+1, count, err)
        }
    }

    return entries, nil
}

func (rr ResponseReader) readMetaframe () (int64, error) {
    read, err := rr.reader.ReadBytes('\n')

    if err != nil {
        return 0, err
    }

    if read[0] != '*' || read[len(read) - 1] != '\n' {
        return 0, ErrInvalidPacket
    }

    log.Printf("    metaframe: %v", read)

    length, err := strconv.ParseInt(string(read[1:len(read) - 1]), 10, 64)
    if err != nil {
        return 0, err
    }

    return length, nil
}

func (rr ResponseReader) readOneEntry() (interface{}, error) {
    read, err := rr.reader.ReadBytes('\n')
    if err != nil {
        return nil, err
    }

    length, err := strconv.ParseInt(string(read[1:len(read) - 1]), 10, 64)
    if err != nil {
        return nil, err
    }
    log.Printf("    type: %c, entry length: %d", protocol.DataType(read[0]), length)
    
    switch protocol.DataType(read[0]) {
    case protocol.DataTypeString: // string
        return rr.readStringValue(length)
    case protocol.DataTypeResponseCode: // resp code
        return rr.readResponseCode(length)
    case protocol.DataTypeBinaryString: // binary_string
        return rr.readBinaryStringValue(length)
    // case '$': // json
    // case '.': // uint8
    // case '-': // int8
    case ':': // uint64
        return rr.readUint64(length)
    case ';': // int64
        return rr.readInt64(length)
    // case '%': // float32
    // // arrays
    // case '&': // recursive array
    // case '_': // flat (non-recursive) array 
    // case '@': // typed array
    // case '~': // any array
    // case '^': // typed non-null array
    default:
        return nil, ErrNotImplementedDataType
    }
}

func (rr ResponseReader) readStringValue(bytes int64) (string, error) {
    var str []byte = make([]byte, bytes)
    _, err := io.ReadFull(rr.reader, str)
    if err != nil {
        return "", err
    }

    log.Printf("    data(str): %v", str)

    return string(str), nil
}

func (rr ResponseReader) readBinaryStringValue(bytes int64) ([]byte, error) {
    var str []byte = make([]byte, bytes)
    _, err := io.ReadFull(rr.reader, str)
    if err != nil {
        return nil, err
    }

    log.Printf("    data(bstr): %v", str)

    return str, nil
}

func (rr ResponseReader) readUint64(chars int64) (uint64, error) {
    read, err := rr.reader.ReadBytes('\n')
    if err != nil {
        return 0, err
    }

    log.Printf("    data(int64): %v", read)

    if len(read) != int(chars + 1) {
        return 0, ErrDataLengthMismatch
    }

    return strconv.ParseUint(string(read[:len(read) - 1]), 10, 64)
}

func (rr ResponseReader) readInt64(chars int64) (int64, error) {
    read, err := rr.reader.ReadBytes('\n')
    if err != nil {
        return 0, err
    }

    log.Printf("    data(int64): %v", read)

    if len(read) != int(chars + 1) {
        return 0, ErrDataLengthMismatch
    }

    return strconv.ParseInt(string(read[:len(read) - 1]), 10, 64)
}

func (rr ResponseReader) readResponseCode(chars int64) (protocol.ResponseCode, error) {
    read, err := rr.reader.ReadBytes('\n')
    if err != nil {
        return -1, err
    }

    log.Printf("    data(resp): %v", read)

    if len(read) != int(chars + 1) {
        return -1, ErrDataLengthMismatch
    }

    if read[0] >= '0' && read[0] <= '9' {
        i, err := strconv.ParseInt(string(read[:len(read) - 1]), 10, 64)
        if err != nil {
            return -1, err
        }

        log.Printf("    i(resp): %d", i)
        return protocol.ResponseCode(i), nil
    } else {
        return -1, protocol.NewServerErrorResponse(string(read[:len(read) - 1]))
    }
}
