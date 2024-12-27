package iterator

import "github.com/feichai0017/NoKV/utils/codec"

// 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
}
type Item interface {
	Entry() *codec.Entry
}
type Options struct {
	Prefix []byte
	IsAsc  bool
}