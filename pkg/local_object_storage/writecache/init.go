package writecache

import (
	"errors"
	"sync"

	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/internal/log"
	meta "github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/TrueCloudLab/frostfs-sdk-go/client/status"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func (c *cache) initFlushMarks() {
	var localWG sync.WaitGroup

	localWG.Add(1)
	go func() {
		defer localWG.Done()

		c.fsTreeFlushMarkUpdate()
	}()

	localWG.Add(1)
	go func() {
		defer localWG.Done()

		c.dbFlushMarkUpdate()
	}()

	c.initWG.Add(1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.initWG.Done()

		localWG.Wait()

		select {
		case <-c.stopInitCh:
			return
		case <-c.closeCh:
			return
		default:
		}

		c.initialized.Store(true)
	}()
}

var errStopIter = errors.New("stop iteration")

func (c *cache) fsTreeFlushMarkUpdate() {
	c.log.Info("filling flush marks for objects in FSTree")

	var prm common.IteratePrm
	prm.LazyHandler = func(addr oid.Address, _ func() ([]byte, error)) error {
		select {
		case <-c.closeCh:
			return errStopIter
		case <-c.stopInitCh:
			return errStopIter
		default:
		}

		flushed, needRemove := c.flushStatus(addr)
		if flushed {
			c.store.flushed.Add(addr.EncodeToString(), true)
			if needRemove {
				var prm common.DeletePrm
				prm.Address = addr

				_, err := c.fsTree.Delete(prm)
				if err == nil {
					storagelog.Write(c.log,
						storagelog.AddressField(addr),
						storagelog.StorageTypeField(wcStorageType),
						storagelog.OpField("fstree DELETE"),
					)
				}
			}
		}
		return nil
	}
	_, _ = c.fsTree.Iterate(prm)
	c.log.Info("finished updating FSTree flush marks")
}

func (c *cache) dbFlushMarkUpdate() {
	c.log.Info("filling flush marks for objects in database")

	var m []string
	var indices []int
	var lastKey []byte
	var batchSize = flushBatchSize
	for {
		select {
		case <-c.closeCh:
			return
		case <-c.stopInitCh:
			return
		default:
		}

		m = m[:0]
		indices = indices[:0]

		// We put objects in batches of fixed size to not interfere with main put cycle a lot.
		_ = c.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(defaultBucket)
			cs := b.Cursor()
			for k, _ := cs.Seek(lastKey); k != nil && len(m) < batchSize; k, _ = cs.Next() {
				m = append(m, string(k))
			}
			return nil
		})

		var addr oid.Address
		for i := range m {
			if err := addr.DecodeString(m[i]); err != nil {
				continue
			}

			flushed, needRemove := c.flushStatus(addr)
			if flushed {
				c.store.flushed.Add(addr.EncodeToString(), true)
				if needRemove {
					indices = append(indices, i)
				}
			}
		}

		if len(m) == 0 {
			break
		}

		err := c.db.Batch(func(tx *bbolt.Tx) error {
			b := tx.Bucket(defaultBucket)
			for _, j := range indices {
				if err := b.Delete([]byte(m[j])); err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			for _, j := range indices {
				storagelog.Write(c.log,
					zap.String("address", m[j]),
					storagelog.StorageTypeField(wcStorageType),
					storagelog.OpField("db DELETE"),
				)
			}
		}
		lastKey = append([]byte(m[len(m)-1]), 0)
	}

	c.log.Info("finished updating flush marks")
}

// flushStatus returns info about the object state in the main storage.
// First return value is true iff object exists.
// Second return value is true iff object can be safely removed.
func (c *cache) flushStatus(addr oid.Address) (bool, bool) {
	var existsPrm meta.ExistsPrm
	existsPrm.SetAddress(addr)

	_, err := c.metabase.Exists(existsPrm)
	if err != nil {
		needRemove := errors.Is(err, meta.ErrObjectIsExpired) || errors.As(err, new(apistatus.ObjectAlreadyRemoved))
		return needRemove, needRemove
	}

	var prm meta.StorageIDPrm
	prm.SetAddress(addr)

	mRes, _ := c.metabase.StorageID(prm)
	res, err := c.blobstor.Exists(common.ExistsPrm{Address: addr, StorageID: mRes.StorageID()})
	return err == nil && res.Exists, false
}
