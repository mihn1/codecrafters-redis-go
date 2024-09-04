package internal

func DecodeValueType(t ValueType) string {
	switch t {
	case ValTypeString:
		return "string"
	case ValTypeList:
		return "list"
	case ValTypeSet:
		return "set"
	case ValTypeZSet:
		return "zset"
	case ValTypeHash:
		return "hash"
	case ValTypeStream:
		return "stream"
	default:
		return "unknown"
	}
}
