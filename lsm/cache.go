package lsm

import "github.com/feichai0017/NoKV/utils"

type cache struct {
	indexs *utils.CoreMap // key fid， value table
	blocks *utils.CoreMap // key cacheID_blockOffset  value block []byte
}
type blockBuffer struct {
	b []byte
}

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{indexs: utils.NewMap(), blocks: utils.NewMap()}
}


// TODO fid 使用字符串是不是会有性能损耗
func (c *cache) addIndex(fid string, t *table) {
	c.indexs.Set(fid, t)
}