package codec

type ValuePtr struct {
}

// NewValuePtr
func NewValuePtr(entry *Entry) *ValuePtr {
	return &ValuePtr{}
}

// IsValuePtr
func IsValuePtr(entry *Entry) bool {
	return false
}

// ValuePtrDecode
func ValuePtrDecode(data []byte) *ValuePtr {
	return nil
}