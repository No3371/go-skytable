package protocol

//go:generate stringer -type=DataType
type DataType byte

const (
	DataTypeUnknown         DataType = 0
	DataTypeString         DataType = '+'
	DataTypeResponseCode   DataType = '!'
	DataTypeJson           DataType = '$'
	DataTypeSmallintSigned DataType = '-'
	DataTypeSmallint       DataType = '.'
	DataTypeIntSigned      DataType = ';'
	DataTypeInt            DataType = ':'
	DataTypeFloat          DataType = '%'
	DataTypeBinaryString   DataType = '?'

	DataTypeArray             DataType = '&'
	DataTypeFlatArray         DataType = '_'
	DataTypeTypedArray        DataType = '@'
	DataTypeAnyArray          DataType = '~'
	DataTypeTypedNonNullArray DataType = '^'
)

//go:generate stringer -type=SimpleType
type SimpleType byte

const (
	SimpleTypeString         SimpleType = SimpleType(DataTypeString)
	SimpleTypeResponseCode   SimpleType = SimpleType(DataTypeResponseCode)
	SimpleTypeJson           SimpleType = SimpleType(DataTypeJson)
	SimpleTypeSmallintSigned SimpleType = SimpleType(DataTypeSmallintSigned)
	SimpleTypeSmallint       SimpleType = SimpleType(DataTypeSmallint)
	SimpleTypeIntSigned      SimpleType = SimpleType(DataTypeIntSigned)
	SimpleTypeInt            SimpleType = SimpleType(DataTypeInt)
	SimpleTypeFloat          SimpleType = SimpleType(DataTypeFloat)
	SimpleTypeBinaryString   SimpleType = SimpleType(DataTypeBinaryString)
)

//go:generate stringer -type=CompoundType
type CompoundType byte

const (
	CompoundTypeArray             CompoundType = CompoundType(DataTypeArray)
	CompoundTypeFlatArray         CompoundType = CompoundType(DataTypeFlatArray)
	CompoundTypeTypedArray        CompoundType = CompoundType(DataTypeTypedArray)
	CompoundTypeAnyArray          CompoundType = CompoundType(DataTypeAnyArray)
	CompoundTypeTypedNonNullArray CompoundType = CompoundType(DataTypeTypedNonNullArray)
)
