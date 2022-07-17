package protocol

type DataType byte

const (
	DataTypeString         DataType = '+'
	DataTypeResponseCode DataType = '!'
	DataTypeJson DataType = '$'
	DataTypeSmallintSigned DataType = '-'
	DataTypeSmallint      DataType = '.'
	DataTypeIntSigned      DataType = ';'
	DataTypeInt            DataType = ';'
	DataTypeFloat            DataType = '%'
	DataTypeBinaryString           DataType = '?'

	DataTypeArray         DataType = '&'
	DataTypeFlatArray         DataType = '_'
	DataTypeTypedArray         DataType = '@'
	DataTypeAnyArray         DataType = '~'
	DataTypeTypedNonNullArray         DataType = '^'
)

type ArrayType byte

const (
	ArrayTypeArray         ArrayType = '&'
	ArrayTypeFlatArray         ArrayType = '_'
	ArrayTypeTypedArray         ArrayType = '@'
	ArrayTypeAnyArray         ArrayType = '~'
	ArrayTypeTypedNonNullArray         ArrayType = '^'
)