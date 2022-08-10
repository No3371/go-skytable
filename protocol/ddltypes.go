package protocol

type DDLDataTypes byte

const (
    DDLDataTypes_String DDLDataTypes = iota
    DDLDataTypes_BinaryString
    DDLDataTypes_List
)

func (dt DDLDataTypes) String () string {
    switch dt {
    case DDLDataTypes_String:
        return "str"
    case DDLDataTypes_BinaryString:
        return "binstr"
    case DDLDataTypes_List:
        return "list"
    default:
        return "?"
    }
}