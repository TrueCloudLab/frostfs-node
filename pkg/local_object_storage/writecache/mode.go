package writecache

import (
	"fmt"
	"time"

	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/shard/mode"
	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/util/logicerr"
)

// ErrReadOnly is returned when Put/Write is performed in a read-only mode.
var ErrReadOnly = logicerr.New("write-cache is in read-only mode")

// ErrNotInitialized is returned when write-cache is initializing.
var ErrNotInitialized = logicerr.New("write-cache is not initialized yet")

// SetMode sets write-cache mode of operation.
// When shard is put in read-only mode all objects in memory are flushed to disk
// and all background jobs are suspended.
func (c *cache) SetMode(m mode.Mode) error {
	c.modeMtx.Lock()
	defer c.modeMtx.Unlock()

	return c.setMode(m)
}

// setMode applies new mode. Must be called with cache.modeMtx lock taken.
func (c *cache) setMode(m mode.Mode) error {
	var err error
	turnOffMeta := m.NoMetabase()

	if turnOffMeta && !c.mode.NoMetabase() {
		err = c.flush(true)
		if err != nil {
			return err
		}
	}

	if !c.initialized.Load() {
		close(c.stopInitCh)

		c.initWG.Wait()
		c.stopInitCh = make(chan struct{})

		defer func() {
			if err == nil && !turnOffMeta {
				c.initFlushMarks()
			}
		}()
	}

	if c.db != nil {
		if err = c.db.Close(); err != nil {
			return fmt.Errorf("can't close write-cache database: %w", err)
		}
	}

	// Suspend producers to ensure there are channel send operations in fly.
	// flushCh is populated by `flush` with `modeMtx` taken, thus waiting until it is empty
	// guarantees that there are no in-fly operations.
	for len(c.flushCh) != 0 {
		c.log.Info("waiting for channels to flush")
		time.Sleep(time.Second)
	}

	if turnOffMeta {
		c.mode = m
		return nil
	}

	if err = c.openStore(m.ReadOnly()); err != nil {
		return err
	}

	c.mode = m
	return nil
}

// readOnly returns true if current mode is read-only.
// `c.modeMtx` must be taken.
func (c *cache) readOnly() bool {
	return c.mode.ReadOnly()
}
