package protocol


type ModelType byte

const (
	ModelTypeKeyMap         ModelType = iota
)

type KeyMapDescription struct {
	KeyType DataType
	ValueType DataType
	Volatile bool
}