package NoKV

import (
	"github.com/feichai0017/NoKV/utils/codec"
	"github.com/feichai0017/NoKV/iterator"
)

type DBIterator struct {
	iters []iterator.Iterator
}
type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}
func (db *DB) NewIterator(opt *iterator.Options) iterator.Iterator {
	dbIter := &DBIterator{}
	dbIter.iters = make([]iterator.Iterator, 0)
	dbIter.iters = append(dbIter.iters, db.lsm.NewIterator(opt))
	return dbIter
}

func (iter *DBIterator) Next() {
	iter.iters[0].Next()
}
func (iter *DBIterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *DBIterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *DBIterator) Item() iterator.Item {
	return iter.iters[0].Item()
}
func (iter *DBIterator) Close() error {
	return nil
}