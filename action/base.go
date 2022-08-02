package action

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

func AppendElement(v interface{}, builder *strings.Builder, typed bool) error {
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
			fmt.Fprintf(builder, "?%d\n", len(string(v)))
		} else {
			fmt.Fprintf(builder, "%d\n", len(string(v)))
		}
		builder.Write(v)
		builder.WriteByte('\n')
	case *protocol.TypedArray:
		if !typed {
			return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
		}

		fmt.Fprintf(builder, "%c%c%d\n", v.ArrayType, v.ElementType, len(v.Elements))
		switch v.ArrayType {
		case protocol.CompoundTypeTypedArray:
			for _, e := range v.Elements {
				err := AppendElement(e, builder, false)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeTypedNonNullArray:
			for _, e := range v.Elements {
				if e == nil {
					return protocol.NewUnexpectedProtocolError("Appending an nil element to non-null typed array ", nil) // NON NULL
				}
				err := AppendElement(e, builder, false)
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
				fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeArray, len(v.Elements))
			} else {
				return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
			}
			for _, e := range v.Elements {
				err := AppendElement(e, builder, true)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeFlatArray:
			if typed {
				fmt.Fprintf(builder, "%c%d\n", protocol.CompoundTypeFlatArray, len(v.Elements))
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

				err := AppendElement(e, builder, false)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeTypedArray:
			// for typed or typed-non-null array, use protocol.TypedArray instead of protocol.Array
			return protocol.ErrIncorrectArrayUsage
		case protocol.CompoundTypeAnyArray:
			if typed {
				fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeAnyArray, len(v.Elements))
			} else {
				return protocol.NewUnexpectedProtocolError("Appending an array without type info", nil)
			}
			for _, e := range v.Elements {
				if e == nil {
					// NON NULL
					return protocol.NewUnexpectedProtocolError("Appending an nil element to an any-array ", nil) // NON NULL
				}
				err := AppendElement(e, builder, false)
				if err != nil {
					return err
				}
			}
		case protocol.CompoundTypeTypedNonNullArray:
			// for typed or typed-non-null array, use protocol.TypedArray instead of protocol.Array
			return protocol.ErrIncorrectArrayUsage
		}
	default:
		return protocol.NewUnexpectedProtocolError("Appending an unexpected element", nil)
	}

	return nil
}

// elementType is only used when it's a TypedArray or TypedNonNullArray
func AppendArrayHeader(arrayType protocol.CompoundType, elementType protocol.DataType, elementCount int, builder *strings.Builder) error {
	switch arrayType {
	case protocol.CompoundTypeArray:
		fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeArray, elementCount)
	case protocol.CompoundTypeFlatArray:
		fmt.Fprintf(builder, "%c%d\n", protocol.CompoundTypeFlatArray, elementCount)
	case protocol.CompoundTypeTypedArray:
		fmt.Fprintf(builder, "%c%c%d\n", protocol.CompoundTypeFlatArray, elementType, elementCount)
	case protocol.CompoundTypeAnyArray:
		fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeAnyArray, elementCount)
	case protocol.CompoundTypeTypedNonNullArray:
		fmt.Fprintf(builder, "%c%d\n", protocol.DataTypeTypedNonNullArray, elementCount)
	default:
		return protocol.NewUnexpectedProtocolError("Appending array header for an unexpected arrayType", nil)
	}

	return nil
}
