package utils

import (
	"fmt"
	"github.com/feichai0017/NoKV/utils/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipList_compare(t *testing.T) {
	list := SkipList{
		header:   nil,
		rand:     nil,
		maxLevel: 0,
		length:   0,
	}

	byte1 := []byte("1")
	byte2 := []byte("2")
	entry1 := codec.NewEntry(byte1, byte1)

	byte1score := list.calcScore(byte1)
	byte2score := list.calcScore(byte2)

	elem := &Element{
		levels: nil,
		entry:  entry1,
		score:  byte2score,
	}

	assert.Equal(t, list.compare(byte1score, byte1, elem), -1)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList()

	//Put & Get
	entry1 := codec.NewEntry([]byte("Key1"), []byte("Val1"))
	assert.Nil(t, list.Add(entry1))
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := codec.NewEntry([]byte("Key2"), []byte("Val2"))
	assert.Nil(t, list.Add(entry2))
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte("noexist")))

	//Update a entry
	entry2_new := codec.NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, list.Add(entry2_new))
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList()
	key, val := "", ""
	maxTime := 1000000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := codec.NewEntry([]byte(key), []byte(val))
		res := list.Add(entry)
		assert.Equal(b, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))

	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList()
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(t, l.Add(codec.NewEntry(key(i), key(i))))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(t, key(i), v.Value)
				return
			}
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList()
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(b, l.Add(codec.NewEntry(key(i), key(i))))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(b, key(i), v.Value)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}