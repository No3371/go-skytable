// Code generated by "stringer -type=SimpleType"; DO NOT EDIT.

package protocol

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[SimpleTypeString-43]
	_ = x[SimpleTypeResponseCode-33]
	_ = x[SimpleTypeJson-36]
	_ = x[SimpleTypeSmallintSigned-45]
	_ = x[SimpleTypeSmallint-46]
	_ = x[SimpleTypeIntSigned-59]
	_ = x[SimpleTypeInt-58]
	_ = x[SimpleTypeFloat-37]
	_ = x[SimpleTypeBinaryString-63]
}

const (
	_SimpleType_name_0 = "SimpleTypeResponseCode"
	_SimpleType_name_1 = "SimpleTypeJsonSimpleTypeFloat"
	_SimpleType_name_2 = "SimpleTypeString"
	_SimpleType_name_3 = "SimpleTypeSmallintSignedSimpleTypeSmallint"
	_SimpleType_name_4 = "SimpleTypeIntSimpleTypeIntSigned"
	_SimpleType_name_5 = "SimpleTypeBinaryString"
)

var (
	_SimpleType_index_1 = [...]uint8{0, 14, 29}
	_SimpleType_index_3 = [...]uint8{0, 24, 42}
	_SimpleType_index_4 = [...]uint8{0, 13, 32}
)

func (i SimpleType) String() string {
	switch {
	case i == 33:
		return _SimpleType_name_0
	case 36 <= i && i <= 37:
		i -= 36
		return _SimpleType_name_1[_SimpleType_index_1[i]:_SimpleType_index_1[i+1]]
	case i == 43:
		return _SimpleType_name_2
	case 45 <= i && i <= 46:
		i -= 45
		return _SimpleType_name_3[_SimpleType_index_3[i]:_SimpleType_index_3[i+1]]
	case 58 <= i && i <= 59:
		i -= 58
		return _SimpleType_name_4[_SimpleType_index_4[i]:_SimpleType_index_4[i+1]]
	case i == 63:
		return _SimpleType_name_5
	default:
		return "SimpleType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
