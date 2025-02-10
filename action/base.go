package action

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/No3371/go-skytable/protocol"
)

type KVPair struct {
	K string
	V any
}

func AppendElements(builder *strings.Builder, typed bool, v ...any) error {
	for _, e := range v {
		err := AppendElement(builder, typed, e)
		if err != nil {
			return err
		}
	}

	return nil
}

func AppendElement(builder *strings.Builder, typed bool, v interface{}) error {
	if v == nil {
		fmt.Fprintf(builder, "\\0\n")
		return nil
	}

	switch v := v.(type) {
	case string:
		if typed {
			builder.WriteRune(rune(protocol.DataTypeString))
			builder.WriteString(strconv.Itoa(len(v)))
			builder.WriteRune('\n')
			builder.WriteString(v)
			builder.WriteRune('\n')
		} else {
			builder.WriteString(strconv.Itoa(len(v)))
			builder.WriteRune('\n')
			builder.WriteString(v)
			builder.WriteRune('\n')
		}
	case int8:
		if typed {
			builder.WriteRune(rune(protocol.DataTypeSmallintSigned))
		}
		if v >= 100 || v <= -100 {
			builder.WriteRune('3')
		} else if v >= 10 || v <= -10 {
			builder.WriteRune('2')
		} else {
			builder.WriteRune('1')
		}
		builder.WriteRune('\n')
		builder.WriteString(strconv.Itoa(int(v)))
		builder.WriteRune('\n')
	case uint8:
		if typed {
			builder.WriteRune(rune(protocol.DataTypeSmallint))
		}
		if v >= 100 {
			builder.WriteRune('3')
		} else if v >= 10 {
			builder.WriteRune('2')
		} else {
			builder.WriteRune('1')
		}
		builder.WriteRune('\n')
		builder.WriteString(strconv.Itoa(int(v)))
		builder.WriteRune('\n')
	case int:
		formated := strconv.FormatInt(int64(v), 10)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeIntSigned))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case int32: // as int64
		formated := strconv.FormatInt(int64(v), 10)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeIntSigned))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case int64:
		formated := strconv.FormatInt(int64(v), 10)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeIntSigned))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case uint: // as uint64
		formated := strconv.FormatUint(uint64(v), 10)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeInt))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case uint32: // as uint64
		formated := strconv.FormatUint(uint64(v), 10)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeInt))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case uint64:
		formated := strconv.FormatUint(uint64(v), 10)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeInt))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case float32:
		formated := strconv.FormatFloat(float64(v), 'f', -1, 64)
		if typed {
			builder.WriteRune(rune(protocol.DataTypeFloat))
		}
		builder.WriteString(strconv.Itoa(len(formated)))
		builder.WriteRune('\n')
		builder.WriteString(formated)
		builder.WriteRune('\n')
	case []byte:
		// ???
		if typed {
			builder.WriteRune(rune(protocol.DataTypeBinaryString))
		}
		builder.WriteString(strconv.Itoa(len(*(*string)(unsafe.Pointer(&v)))))
		builder.WriteRune('\n')
		builder.Write(v)
		builder.WriteByte('\n')
	case *protocol.TypedArray:
		if !typed {
			return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
		}
		builder.WriteRune(rune(v.ArrayType))
		builder.WriteRune(rune(v.ElementType))
		builder.WriteString(strconv.Itoa(len(v.Elements)))
		builder.WriteRune('\n')
		switch v.ArrayType {
		case protocol.CompoundTypeTypedArray:
			for _, e := range v.Elements {
				err := AppendElement(builder, false, e)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeTypedNonNullArray:
			for _, e := range v.Elements {
				if e == nil {
					return protocol.NewUnexpectedProtocolError("Appending an nil element to non-null typed array ", nil) // NON NULL
				}
				err := AppendElement(builder, false, e)
				if err != nil {
					return err
				}
			}
		default:
			return protocol.ErrIncorrectArrayUsage
		}
	case *protocol.Array:
		switch v.ArrayType {
		case protocol.CompoundTypeArray:
			if typed {
				builder.WriteRune(rune(protocol.DataTypeArray))
				builder.WriteString(strconv.Itoa(len(v.Elements)))
				builder.WriteRune('\n')
			} else {
				return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
			}
			for _, e := range v.Elements {
				err := AppendElement(builder, true, e)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeFlatArray:
			if typed {
				builder.WriteRune(rune(protocol.CompoundTypeFlatArray))
				builder.WriteString(strconv.Itoa(len(v.Elements)))
				builder.WriteRune('\n')
			} else {
				return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
			}

			for _, e := range v.Elements {
				switch e.(type) {
				case protocol.Array:
					return protocol.NewUnexpectedProtocolError("Appending an flat-array containing another array", nil)
				case protocol.TypedArray:
					return protocol.NewUnexpectedProtocolError("Appending an flat-array containing another array", nil)
				}

				err := AppendElement(builder, false, e)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeTypedArray:
			// for typed or typed-non-null array, use protocol.TypedArray instead of protocol.Array
			return protocol.ErrIncorrectArrayUsage
		case protocol.CompoundTypeAnyArray:
			if typed {
				builder.WriteRune(rune(protocol.DataTypeAnyArray))
				builder.WriteString(strconv.Itoa(len(v.Elements)))
				builder.WriteRune('\n')
			} else {
				return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
			}
			for _, e := range v.Elements {
				if e == nil {
					// NON NULL
					return protocol.NewUnexpectedProtocolError("Appending an nil element to an any-array ", nil) // NON NULL
				}
				err := AppendElement(builder, false, e)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeTypedNonNullArray:
			// for typed or typed-non-null array, use protocol.TypedArray instead of protocol.Array
			return protocol.ErrIncorrectArrayUsage
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("Appending an unexpected element: %v (%T)", v, v), nil)
	}

	return nil
}

// elementType is only used when it's a TypedArray or TypedNonNullArray
func AppendArrayHeader(arrayType protocol.CompoundType, elementType protocol.DataType, elementCount int, builder *strings.Builder) error {
	switch arrayType {
	case protocol.CompoundTypeArray:
		builder.WriteRune(rune(protocol.DataTypeArray))
		builder.WriteString(strconv.Itoa(elementCount))
		builder.WriteRune('\n')
	case protocol.CompoundTypeFlatArray:
		builder.WriteRune(rune(protocol.CompoundTypeFlatArray))
		builder.WriteString(strconv.Itoa(elementCount))
		builder.WriteRune('\n')
	case protocol.CompoundTypeTypedArray:
		builder.WriteRune(rune(protocol.CompoundTypeFlatArray))
		builder.WriteRune(rune(elementType))
		builder.WriteString(strconv.Itoa(elementCount))
		builder.WriteRune('\n')
	case protocol.CompoundTypeAnyArray:
		builder.WriteRune(rune(protocol.DataTypeAnyArray))
		builder.WriteString(strconv.Itoa(elementCount))
		builder.WriteRune('\n')
	case protocol.CompoundTypeTypedNonNullArray:
		builder.WriteRune(rune(protocol.DataTypeTypedNonNullArray))
		builder.WriteString(strconv.Itoa(elementCount))
		builder.WriteRune('\n')
	default:
		return protocol.NewUnexpectedProtocolError("Appending array header for an unexpected arrayType", nil)
	}

	return nil
}
