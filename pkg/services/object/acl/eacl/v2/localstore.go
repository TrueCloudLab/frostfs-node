package v2

import (
	"io"

	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/engine"
	objectSDK "github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

type localStorage engine.StorageEngine

func (s *localStorage) Head(addr oid.Address) (*objectSDK.Object, error) {
	if s == nil {
		return nil, io.ErrUnexpectedEOF
	}

	return engine.Head((*engine.StorageEngine)(s), addr)
}
