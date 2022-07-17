package protocol

type Array struct {
    ArrayType ArrayType
    Elements []interface{}
}

type TypedArray struct {
    Array
    ElementType DataType
}
