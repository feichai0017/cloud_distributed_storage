package lsm

import (
	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/utils/codec"
)

type levelManager struct {
	opt      *Options
	cache    *cache
	manifest *file.Manifest
	levels   []*levelHandler
}

type levelHandler struct {
	levelNum int
	tables   []*table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	// 如果是第0层文件则进行特殊处理
	if lh.levelNum == 0 {
		// logic...
	} else {
		// logic...
	}
	return nil, nil
}
func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifest.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	// 读取manifest文件构建管理器
	lm.loadManifest()
	lm.build()
	return lm
}
func (lm *levelManager) loadCache() {
	lm.cache = newCache(lm.opt)
	// 添加 idx cache
	for _, level := range lm.levels {
		for _, table := range level.tables {
			lm.cache.addIndex(table.ss.FID(), table)
		}
	}
}
func (lm *levelManager) loadManifest() {
	lm.manifest = file.OpenManifest(&file.Options{Name: "manifest", Dir: lm.opt.WorkDir})
}
func (lm *levelManager) build() {
	// 如果manifest文件是空的 则进行初始化
	lm.levels = make([]*levelHandler, utils.MaxLevelNum)
	tables := lm.manifest.Tables()
	for num := 0; num < utils.MaxLevelNum; num++ {
		lm.levels[num] = &levelHandler{levelNum: num}
		lm.levels[num].tables = make([]*table, len(tables[num]))
		for i := range tables[num] {
			lm.levels[num].tables[i] = openTable(lm.opt, tables[num][i])
		}
	}
	// 逐一加载sstable 的index block 构建cache
	lm.loadCache()
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) error {
	// flush 跳表中的数据转化为sst文件
	// 删除wal文件并创建一个新的wal文件
	return nil
}

func (lm *levelManager) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// L0层查询
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7层查询
	for level := 1; level < utils.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, nil
}