package NoKV

import (
	"github.com/feichai0017/NoKV/iterator"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/utils/codec"
	"github.com/feichai0017/NoKV/vlog"
)

type (
	// coreKV对外提供的功能集合
	CoreAPI interface {
		Set(data *codec.Entry) error
		Get(key []byte) (*codec.Entry, error)
		Del(key []byte) error
		NewIterator(opt *iterator.Options) iterator.Iterator
		Info() *Stats
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
	DB struct {
		opt   *Options
		lsm   *lsm.LSM
		vlog  *vlog.VLog
		stats *Stats
	}
)

func Open(options *Options) *DB {
	db := &DB{opt: options}
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{})
	// 初始化vlog结构
	db.vlog = vlog.NewVLog(&vlog.Options{})
	// 初始化统计信息
	db.stats = newStats(options)
	// 启动 sstable 的合并压缩过程
	go db.lsm.StartMerge()
	// 启动 vlog gc 过程
	go db.vlog.StartGC()
	// 启动 info 统计过程
	go db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	if err := db.stats.close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	return db.Set(&codec.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}
func (db *DB) Set(data *codec.Entry) error {
	// 做一些必要性的检查
	// 如果value 大于一个阈值 则创建值指针，并将其写入vlog中
	var valuePtr *codec.ValuePtr
	if utils.ValueSize(data.Value) > db.opt.ValueThreshold {
		valuePtr = codec.NewValuePtr(data)
		// 先写入vlog不会有事务问题，因为如果lsm写入失败，vlog会在GC阶段清理无效的key
		if err := db.vlog.Set(data); err != nil {
			return err
		}
	}
	// 写入LSM, 如果写值指针不空则替换值entry.value的值
	if valuePtr != nil {
		data.Value = codec.ValuePtrCodec(valuePtr)
	}
	return db.lsm.Set(data)
}
func (db *DB) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// 检查输入
	// 从内存表中读取数据
	if entry, err = db.lsm.Get(key); err == nil {
		return entry, err
	}
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	if entry != nil && codec.IsValuePtr(entry) {
		if entry, err = db.vlog.Get(entry); err == nil {
			return entry, err
		}
	}
	return nil, nil
}
func (db *DB) Info() *Stats {
	// 读取stats结构，打包数据并返回
	return db.stats
}