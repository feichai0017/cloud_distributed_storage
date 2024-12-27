package lsm

import (
	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/utils/codec"
)

// MemTable
type memTable struct {
	wal *file.WalFile
	sl  *utils.SkipList
}

//todo: mock, need to add real logic
func NewMemtable() (*memTable, error) {

	return nil, nil
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

func (m *memTable) set(entry *codec.Entry) error {
	// 写到wal 日志中，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	if err := m.sl.Add(entry); err != nil {
		return err
	}
	return nil
}

func (m *memTable) Get(key []byte) (*codec.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	return m.sl.Search(key), nil
}

func (m *memTable) Size() int64 {
	return m.sl.Size()
}

//recovery
func recovery(opt *Options) (*memTable, []*memTable) {
	fileOpt := &file.Options{}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}
}