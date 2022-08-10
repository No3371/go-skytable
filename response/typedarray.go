package response

import (
	"fmt"

	"github.com/No3371/go-skytable/protocol"
)

func (rr ResponseReader) readTypedArray(t protocol.SimpleType, items int64) (*protocol.TypedArray, error) {
	arr := protocol.TypedArray{
		Array: protocol.Array{
			ArrayType: protocol.CompoundTypeTypedArray,
			Elements:  make([]interface{}, items),
		},
		ElementType: t,
	}

	var err error

	for i := int64(0); i < items; i++ {
		arr.Elements[i], err = rr.readOneTypedEntry(protocol.DataType(t))
		if err != nil {
			return &arr, fmt.Errorf("failed to read typed array entry #%d/%d: %w", i + 1, items, err)
		}
	}

	return &arr, nil
}

func (rr ResponseReader) readTypedNonNullArray(t protocol.SimpleType, items int64) (*protocol.TypedArray, error) {
	arr := protocol.TypedArray{
		Array: protocol.Array{
			ArrayType: protocol.CompoundTypeTypedNonNullArray,
			Elements:  make([]interface{}, items),
		},
		ElementType: t,
	}

	var err error

	for i := int64(0); i < items; i++ {
		arr.Elements[i], err = rr.readOneTypedEntry(protocol.DataType(t))
		if err != nil {
			return &arr, fmt.Errorf("failed to read typed array entry #%d/%d: %w", i + 1, items, err)
		}

		if arr.Elements[i] == nil {
			return &arr, fmt.Errorf("read a NULL in a typed non-null array (#%d/%d)", i+1, items)
		}
	}

	return &arr, nil
}