package response

import "github.com/No3371/go-skytable/protocol"

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
		var dt protocol.DataType
		dt, arr.Elements[i], err = rr.readOneEntry()
		if err != nil {
			return &arr, err
		}
		if dt != protocol.DataType(t) {
			return &arr, protocol.ErrUnexpectedProtocol
		}
	}

	return &arr, nil
}