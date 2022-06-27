package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr []oid.Address
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct{}

// WithAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddresses(addr ...oid.Address) {
	if p != nil {
		p.addr = append(p.addr, addr...)
	}
}

// Delete removes data from the shard's writeCache, metaBase and
// blobStor.
func (s *Shard) Delete(prm DeletePrm) (DeleteRes, error) {
	mode := s.GetMode()
	if s.GetMode()&ModeReadOnly != 0 {
		return DeleteRes{}, ErrReadOnlyMode
	}

	ln := len(prm.addr)
	var delSmallPrm blobstor.DeleteSmallPrm
	var delBigPrm blobstor.DeleteBigPrm

	smalls := make(map[oid.Address]*blobovnicza.ID, ln)

	for i := range prm.addr {
		if s.hasWriteCache() {
			err := s.writeCache.Delete(prm.addr[i])
			if err != nil && !writecache.IsErrNotFound(err) {
				s.log.Error("can't delete object from write cache", zap.String("error", err.Error()))
			}
		}

		blobovniczaID, err := meta.IsSmall(s.metaBase, prm.addr[i])
		if err != nil {
			s.log.Debug("can't get blobovniczaID from metabase",
				zap.Stringer("object", prm.addr[i]),
				zap.String("error", err.Error()))

			continue
		}

		if blobovniczaID != nil {
			smalls[prm.addr[i]] = blobovniczaID
		}
	}

	var err error
	if mode&ModeDegraded == 0 { // Skip metabase errors in degraded mode.
		err = meta.Delete(s.metaBase, prm.addr...)
		if err != nil {
			return DeleteRes{}, err // stop on metabase error ?
		}
	}

	for i := range prm.addr { // delete small object
		if id, ok := smalls[prm.addr[i]]; ok {
			delSmallPrm.SetAddress(prm.addr[i])
			delSmallPrm.SetBlobovniczaID(id)

			_, err = s.blobStor.DeleteSmall(delSmallPrm)
			if err != nil {
				s.log.Debug("can't remove small object from blobStor",
					zap.Stringer("object_address", prm.addr[i]),
					zap.String("error", err.Error()))
			}

			continue
		}

		// delete big object

		delBigPrm.SetAddress(prm.addr[i])

		_, err = s.blobStor.DeleteBig(delBigPrm)
		if err != nil {
			s.log.Debug("can't remove big object from blobStor",
				zap.Stringer("object_address", prm.addr[i]),
				zap.String("error", err.Error()))
		}
	}

	return DeleteRes{}, nil
}
