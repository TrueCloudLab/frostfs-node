package v2

import (
	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/engine"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
)

func WithObjectStorage(v ObjectStorage) Option {
	return func(c *cfg) {
		c.storage = v
	}
}

func WithLocalObjectStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.storage = &localStorage{
			ls: v,
		}
	}
}

func WithServiceRequest(v Request) Option {
	return func(c *cfg) {
		c.msg = requestXHeaderSource{
			req: v,
		}
	}
}

func WithServiceResponse(resp Response, req Request) Option {
	return func(c *cfg) {
		c.msg = responseXHeaderSource{
			resp: resp,
			req:  req,
		}
	}
}

func WithCID(v cid.ID) Option {
	return func(c *cfg) {
		c.cnr = v
	}
}

func WithOID(v *oid.ID) Option {
	return func(c *cfg) {
		c.obj = v
	}
}
