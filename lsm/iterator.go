package lsm

import (
	"github.com/feichai0017/NoKV/iterator"
	"github.com/feichai0017/NoKV/utils/codec"
)

type Iterator struct {
	it    iterator.Item
	iters []iterator.Iterator
}
type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}

// 创建迭代器
func (lsm *LSM) NewIterator(opt *iterator.Options) iterator.Iterator {
	iter := &Iterator{}
	iter.iters = make([]iterator.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, imm := range lsm.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.NewIterator(opt))
	return iter
}
func (iter *Iterator) Next() {
	iter.iters[0].Next()
}
func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *Iterator) Item() iterator.Item {
	return iter.iters[0].Item()
}
func (iter *Iterator) Close() error {
	return nil
}

// 内存表迭代器
type memIterator struct {
	innerIter iterator.Iterator
}

func (m *memTable) NewIterator(opt *iterator.Options) iterator.Iterator {
	return &memIterator{innerIter: m.sl.NewSkipListIterator()}
}
func (iter *memIterator) Next() {
	iter.innerIter.Next()
}
func (iter *memIterator) Valid() bool {
	return iter.innerIter.Valid()
}
func (iter *memIterator) Rewind() {
	iter.innerIter.Rewind()
}
func (iter *memIterator) Item() iterator.Item {
	return iter.innerIter.Item()
}
func (iter *memIterator) Close() error {
	return iter.innerIter.Close()
}

// levelManager上的迭代器
type levelIterator struct {
	it    *iterator.Item
	iters []*Iterator
}

func (lm *levelManager) NewIterator(options *iterator.Options) iterator.Iterator {
	return &levelIterator{}
}
func (iter *levelIterator) Next() {
}
func (iter *levelIterator) Valid() bool {
	return false
}
func (iter *levelIterator) Rewind() {

}
func (iter *levelIterator) Item() iterator.Item {
	return &Item{}
}
func (iter *levelIterator) Close() error {
	return nil
}