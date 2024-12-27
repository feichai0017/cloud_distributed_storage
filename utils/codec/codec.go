package codec

// WalCodec 写入wal文件的编码
func WalCodec(entry *Entry) []byte {
	return []byte{}
}

func ValuePtrCodec(ptr *ValuePtr) []byte {
	return []byte{}
}