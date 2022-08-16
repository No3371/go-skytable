package protocol

import (
	"errors"
	"fmt"
	"regexp"
)

var ErrInvalidModelDescriptionString = errors.New("invalid model description string")

var keymapRegex *regexp.Regexp = regexp.MustCompile(`Keymap\s?{\s?data:\s?\((.+),(.+)\),\s?volatile:\s?(true|false)\s?}`)

type ModelType byte

const (
	ModelTypeKeyMap ModelType = iota
)

type ModelDescription interface {
	Model() string
	Properties() string
}

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

func ParseDescription (descStr string) (ModelDescription, error) {
	if matches := keymapRegex.FindStringSubmatch(descStr); matches != nil {
		desc := KeyMapDescription{}
		switch matches[1] {
		case DDLDataTypes_String.String():
			desc.KeyType = DDLDataTypes_String
		case DDLDataTypes_BinaryString.String():
			desc.KeyType = DDLDataTypes_BinaryString
		default:
			return nil, ErrInvalidModelDescriptionString
		}
		switch matches[2] {
		case DDLDataTypes_String.String():
			desc.ValueType = DDLDataTypes_String
		case DDLDataTypes_BinaryString.String():
			desc.ValueType = DDLDataTypes_BinaryString
		case DDLDataTypes_List.String():
			desc.ValueType = DDLDataTypes_List
		default:
			return nil, ErrInvalidModelDescriptionString
		}
		if matches[3] == "true" {
			desc.Volatile = true
		}
		return desc, nil
	}

	return nil, ErrInvalidModelDescriptionString
}