package response

import (
	"fmt"

	"github.com/No3371/go-skytable/protocol"
)

func (rr ResponseReader) readFlatArray(items int64) (*protocol.Array, error) {
	arr := protocol.Array{
		ArrayType: protocol.CompoundTypeFlatArray,
		Elements:  make([]interface{}, items),
	}

	var err error

	for i := int64(0); i < items; i++ {
		var dt protocol.DataType
		dt, arr.Elements[i], err = rr.readOneEntry()
		if err != nil {
			return &arr, fmt.Errorf("failed to read flat array entry #%d/%d: %w", i + 1, items, err)
		}
		if dt.IsCompoundType() {
			return &arr, fmt.Errorf("read a compund type entry (#%d/%d) in a flat array", i+1, items)
		}
	}

	return &arr, nil
}