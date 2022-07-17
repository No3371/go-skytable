package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

func AppendValue (v interface{}, builder *strings.Builder, typed bool) error {
	if v == nil {
		fmt.Fprintf(builder, "\\0\n")
		return nil
	}

	switch v := v.(type) {
	case string:
		if typed {
			fmt.Fprintf(builder, "+%d\n%s\n", len(v), v)
		} else {
			fmt.Fprintf(builder, "%d\n%s\n", len(v), v)
		}
	case int8:
		if typed {
			if v >= 100 || v <= -100 {
				fmt.Fprintf(builder, "-3\n%d\n", v)
			} else if v >= 10 || v <= -10 {
				fmt.Fprintf(builder, "-2\n%d\n", v)
			} else {
				fmt.Fprintf(builder, "-1\n%d\n", v)
			}
		} else {
			if v >= 100 || v <= -100 {
				fmt.Fprintf(builder, "3\n%d\n", v)
			} else if v >= 10 || v <= -10 {
				fmt.Fprintf(builder, "2\n%d\n", v)
			} else {
				fmt.Fprintf(builder, "1\n%d\n", v)
			}
		}
	case uint8:
		if typed {
			if v >= 100 {
				fmt.Fprintf(builder, ".3\n%d\n", v)
			} else if v >= 10 {
				fmt.Fprintf(builder, ".2\n%d\n", v)
			} else {
				fmt.Fprintf(builder, ".1\n%d\n", v)
			}
		} else {
			if v >= 100 {
				fmt.Fprintf(builder, "3\n%d\n", v)
			} else if v >= 10 {
				fmt.Fprintf(builder, "2\n%d\n", v)
			} else {
				fmt.Fprintf(builder, "1\n%d\n", v)
			}
		}
	case int32:
		if typed {
			fmt.Fprintf(builder, ";%d\n%d\n", int32(math.Log10(float64(v)))+1, v)
		} else {
			fmt.Fprintf(builder, "%d\n%d\n", int32(math.Log10(float64(v)))+1, v)
		}
	case uint32:
		if typed {
			fmt.Fprintf(builder, ":%d\n%d\n", uint32(math.Log10(float64(v)))+1, v)
		} else {
			fmt.Fprintf(builder, "%d\n%d\n", uint32(math.Log10(float64(v)))+1, v)
		}
	case float32:
		if typed {
			fmt.Fprintf(builder, "%%%d\n%f\n", len(strconv.FormatFloat(float64(v), 'f', -1, 32)), v)
		} else {
			fmt.Fprintf(builder, "%d\n%f\n", len(strconv.FormatFloat(float64(v), 'f', -1, 32)), v)
		}
	case []byte:
		// ???
		if typed {
			fmt.Fprintf(builder, "?%d\n%s\n", len(string(v)), string(v))
		} else {
			fmt.Fprintf(builder, "%d\n%s\n", len(string(v)), string(v))
		}
	case *protocol.TypedArray:
		if !typed {
			return protocol.ErrUnexpectedProtocol
		}

		fmt.Fprintf(builder, "%c%c%d\n", v.ArrayType, v.ElementType, len(v.Elements))
		switch v.ArrayType {
		case protocol.ArrayTypeTypedArray:
			for _, e := range v.Elements {
				err := AppendValue(e, builder, false)
				if err != nil {
					return err
				}
			}
		case protocol.ArrayTypeTypedNonNullArray:
			for _, e := range v.Elements {
				if e == nil {
					return protocol.ErrUnexpectedProtocol // NON NULL
				}
				err := AppendValue(e, builder, false)
				if err != nil {
					return err
				}
			}
		default:
			return protocol.ErrIncorrectArrayUsage
		}
	case *protocol.Array:
		switch v.ArrayType {
		case protocol.ArrayTypeArray:
			if typed {
				fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeArray, len(v.Elements))
			} else {
				return protocol.ErrUnexpectedProtocol
			}
			for _, e := range v.Elements {
				err := AppendValue(e, builder, true)
				if err != nil {
					return err
				}
			}
		case protocol.ArrayTypeFlatArray:
			if typed {
				fmt.Fprintf(builder, "%c%d\n", protocol.ArrayTypeFlatArray, len(v.Elements))
			} else {
				return protocol.ErrUnexpectedProtocol
			}

			for _, e := range v.Elements {
				switch e.(type) {
				case protocol.Array:
					return protocol.ErrUnexpectedProtocol
				case protocol.TypedArray:
					return protocol.ErrUnexpectedProtocol
				}

				err := AppendValue(e, builder, false)
				if err != nil {
					return err
				}
			}
		case protocol.ArrayTypeTypedArray:
			// for typed or typed-non-null array, use protocol.TypedArray instead of protocol.Array
			return protocol.ErrIncorrectArrayUsage
		case protocol.ArrayTypeAnyArray:
			if typed {
				fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeAnyArray, len(v.Elements))
			} else {
				return protocol.ErrUnexpectedProtocol
			}
			for _, e := range v.Elements {
				if e == nil {
					// NON NULL
					return protocol.ErrUnexpectedProtocol
				}
				err := AppendValue(e, builder, false)
				if err != nil {
					return err
				}
			}
		case protocol.ArrayTypeTypedNonNullArray:
			// for typed or typed-non-null array, use protocol.TypedArray instead of protocol.Array
			return protocol.ErrIncorrectArrayUsage
		}
	default:
		return protocol.ErrUnexpectedProtocol
	}

	return nil
}

// elementType is only used when it's a TypedArray or TypedNonNullArray
func AppendArrayHeader (arrayType protocol.ArrayType, elementType protocol.DataType, elementCount int, builder *strings.Builder) error {
	switch arrayType {
		case protocol.ArrayTypeArray:
			fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeArray, elementCount)
		case protocol.ArrayTypeFlatArray:
			fmt.Fprintf(builder, "%c%d\n", protocol.ArrayTypeFlatArray, elementCount)
		case protocol.ArrayTypeTypedArray:
			fmt.Fprintf(builder, "%c%c%d\n", protocol.ArrayTypeFlatArray, elementType, elementCount)
		case protocol.ArrayTypeAnyArray:
			fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeAnyArray, elementCount)
		case protocol.ArrayTypeTypedNonNullArray:
			fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeTypedNonNullArray, elementCount)
		default:
			return protocol.ErrUnexpectedProtocol
	}

	return nil
}