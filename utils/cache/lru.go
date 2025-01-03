package cache

import (
	"container/list"
	"fmt"
)

type windowLRU struct {
	data map[uint64]*list.Element
	cap int
	list *list.List
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (w *windowLRU) add(s storeItem) (eitem storeItem, evicted bool) {
	// if window is not full, add to window
	if w.list.Len() < w.cap {
		w.data[s.key] = w.list.PushFront(&s)
		return storeItem{}, false
	}

	// if window is full, evict the oldest item
	evictItem := w.list.Back()
	item := evictItem.Value.(*storeItem)
	// remove from data
	delete(w.data, item.key)

	eitem, *item = *item, s
	w.data[item.key] = evictItem
	w.list.MoveToFront(evictItem)
	return eitem, true
}

func (lru *windowLRU) get(v *list.Element) {
	lru.list.MoveToFront(v)
}

func (lru *windowLRU) String() string {
	var s string
	for e := lru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}