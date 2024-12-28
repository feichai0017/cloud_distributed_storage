package utils

import (
	"github.com/feichai0017/NoKV/utils/codec"
	"math/rand"
	"sync"
	"errors"
	"bytes"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel,
		rand:     r,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return errors.New("invalid entry")
	}
	
	list.lock.Lock()
	defer list.lock.Unlock()

	score := list.calcScore(data.Key)
	var elem *Element

	max := len(list.header.levels)
	prev := list.header

	prevElementHeaders := make([]*Element, defaultMaxLevel)

	// 查找阶段
	for i := max - 1; i >= 0; i-- {
		prevElementHeaders[i] = prev
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					next.entry = data
					list.size += int64(len(data.Value))
					return nil
				}
				break
			}
			prev = next
			prevElementHeaders[i] = prev
		}
	}

	// 获取随机层数
	level := list.randLevel()
	if level <= 0 || level > list.maxLevel {
		level = 1
	}

	// 创建新节点
	elem = newElement(score, data, level)
	if elem == nil {
		return errors.New("failed to allocate new element")
	}

	// 更新指针
	for i := 0; i < level; i++ {
		elem.levels[i] = prevElementHeaders[i].levels[i]
		prevElementHeaders[i].levels[i] = elem
	}

	// 更新统计信息
	list.length++
	list.size += int64(len(data.Key) + len(data.Value))

	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	if list.length == 0 {
		return nil
	}

	list.lock.RLock()
	defer list.lock.RUnlock()

	score := list.calcScore(key)

	prev := list.header
	max := len(prev.levels)

	for i := max - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return next.entry
				}
				break
			}
			prev = next
		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	}
	return 1
}

func (list *SkipList) randLevel() int {
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}
	return list.maxLevel - 1
}

func (list *SkipList) Size() int64 {
	return list.size
}