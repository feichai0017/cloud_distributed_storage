package lsm

import "github.com/feichai0017/NoKV/file"

type table struct {
	ss   *file.SSTable
	idxs []byte
}

func openTable(opt *Options, tableName string) *table {
	t := &table{ss: file.OpenSStable(&file.Options{Name: tableName, Dir: opt.WorkDir})}
	// 加载ss文件 索引
	t.idxs = t.ss.Indexs()
	return t
}