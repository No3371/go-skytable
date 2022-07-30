package protocol

type Array struct {
	ArrayType CompoundType
	Elements  []interface{}
}

type TypedArray struct {
	Array
	ElementType SimpleType
}
