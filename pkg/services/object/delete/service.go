package deletesvc

import (
	"github.com/TrueCloudLab/frostfs-node/pkg/core/netmap"
	getsvc "github.com/TrueCloudLab/frostfs-node/pkg/services/object/get"
	putsvc "github.com/TrueCloudLab/frostfs-node/pkg/services/object/put"
	searchsvc "github.com/TrueCloudLab/frostfs-node/pkg/services/object/search"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object/util"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	"github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"github.com/TrueCloudLab/frostfs-sdk-go/user"
	"go.uber.org/zap"
)

// Service utility serving requests of Object.Get service.
type Service struct {
	cfg
}

// Option is a Service's constructor option.
type Option func(*cfg)

// NetworkInfo wraps network state and configurations.
type NetworkInfo interface {
	netmap.State

	// Must return the lifespan of the tombstones
	// in the FrostFS epochs.
	TombstoneLifetime() (uint64, error)

	// Returns user ID of the local storage node. Result must not be nil.
	// New tombstone objects will have the result as an owner ID if removal is executed w/o a session.
	LocalNodeID() user.ID
}

type cfg struct {
	log *logger.Logger

	header interface {
		// must return (nil, nil) for PHY objects
		splitInfo(*execCtx) (*object.SplitInfo, error)

		children(*execCtx) ([]oid.ID, error)

		// must return (nil, nil) for 1st object in chain
		previous(*execCtx, oid.ID) (*oid.ID, error)
	}

	searcher interface {
		splitMembers(*execCtx) ([]oid.ID, error)
	}

	placer interface {
		put(*execCtx) (*oid.ID, error)
	}

	netInfo NetworkInfo

	keyStorage *util.KeyStorage
}

func (c *cfg) initDefault() {
	c.log = &logger.Logger{Logger: zap.L()}
}

// New creates, initializes and returns utility serving
// Object.Get service requests.
func New(opts ...Option) *Service {
	var s Service
	s.cfg.initDefault()

	for i := range opts {
		opts[i](&s.cfg)
	}

	return &s
}

// WithLogger returns option to specify Delete service's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = &logger.Logger{Logger: l.With(zap.String("component", "Object.Delete service"))}
	}
}

// WithHeadService returns option to set Head service
// to work with object headers.
func WithHeadService(h *getsvc.Service) Option {
	return func(c *cfg) {
		c.header = (*headSvcWrapper)(h)
	}
}

// WithSearchService returns option to set search service.
func WithSearchService(s *searchsvc.Service) Option {
	return func(c *cfg) {
		c.searcher = (*searchSvcWrapper)(s)
	}
}

// WithPutService returns option to specify put service.
func WithPutService(p *putsvc.Service) Option {
	return func(c *cfg) {
		c.placer = (*putSvcWrapper)(p)
	}
}

// WithNetworkInfo returns option to set network information source.
func WithNetworkInfo(netInfo NetworkInfo) Option {
	return func(c *cfg) {
		c.netInfo = netInfo
	}
}

// WithKeyStorage returns option to set local private key storage.
func WithKeyStorage(ks *util.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = ks
	}
}
