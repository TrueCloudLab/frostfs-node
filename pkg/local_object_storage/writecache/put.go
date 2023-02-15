package writecache

import (
	"errors"

	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/internal/log"
	"go.etcd.io/bbolt"
)

var (
	// ErrBigObject is returned when object is too big to be placed in cache.
	ErrBigObject = errors.New("too big object")
	// ErrOutOfSpace is returned when there is no space left to put a new object.
	ErrOutOfSpace = errors.New("no space left in the write cache")
)

// Put puts object to write-cache.
//
// Returns ErrReadOnly if write-cache is in R/O mode.
// Returns ErrNotInitialized if write-cache has not been initialized yet.
// Returns ErrOutOfSpace if saving an object leads to WC's size overflow.
// Returns ErrBigObject if an objects exceeds maximum object size.
func (c *cache) Put(prm common.PutPrm) (common.PutRes, error) {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if c.readOnly() {
		return common.PutRes{}, ErrReadOnly
	} else if !c.initialized.Load() {
		return common.PutRes{}, ErrNotInitialized
	}

	sz := uint64(len(prm.RawData))
	if sz > c.maxObjectSize {
		return common.PutRes{}, ErrBigObject
	}

	oi := objectInfo{
		addr: prm.Address.EncodeToString(),
		obj:  prm.Object,
		data: prm.RawData,
	}

	if sz <= c.smallObjectSize {
		return common.PutRes{}, c.putSmall(oi)
	}
	return common.PutRes{}, c.putBig(oi.addr, prm)
}

// putSmall persists small objects to the write-cache database and
// pushes the to the flush workers queue.
func (c *cache) putSmall(obj objectInfo) error {
	cacheSize := c.estimateCacheSize()
	if c.maxCacheSize < c.incSizeDB(cacheSize) {
		return ErrOutOfSpace
	}

	err := c.db.Batch(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.Put([]byte(obj.addr), obj.data)
	})
	if err == nil {
		storagelog.Write(c.log,
			storagelog.AddressField(obj.addr),
			storagelog.StorageTypeField(wcStorageType),
			storagelog.OpField("db PUT"),
		)
		c.objCounters.IncDB()
	}
	return nil
}

// putBig writes object to FSTree and pushes it to the flush workers queue.
func (c *cache) putBig(addr string, prm common.PutPrm) error {
	cacheSz := c.estimateCacheSize()
	if c.maxCacheSize < c.incSizeFS(cacheSz) {
		return ErrOutOfSpace
	}

	_, err := c.fsTree.Put(prm)
	if err != nil {
		return err
	}

	if c.blobstor.NeedsCompression(prm.Object) {
		c.mtx.Lock()
		c.compressFlags[addr] = struct{}{}
		c.mtx.Unlock()
	}
	c.objCounters.IncFS()
	storagelog.Write(c.log,
		storagelog.AddressField(addr),
		storagelog.StorageTypeField(wcStorageType),
		storagelog.OpField("fstree PUT"),
	)

	return nil
}
