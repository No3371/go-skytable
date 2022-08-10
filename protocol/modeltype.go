package protocol

import (
	"fmt"
)

type ModelType byte

const (
	ModelTypeKeyMap ModelType = iota
)

type KeyMapDescription struct {
	KeyType   DDLDataTypes
	ValueType DDLDataTypes
	Volatile  bool
}

func (d KeyMapDescription) Model() string {
	return fmt.Sprintf("keymap(%s,%s)", d.KeyType, d.ValueType)
}

func (d KeyMapDescription) Properties() string {
	if d.Volatile {
		return "volatile"
	} else {
		return ""
	}
}