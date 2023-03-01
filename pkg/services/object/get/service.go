package getsvc

import (
	"github.com/TrueCloudLab/frostfs-node/pkg/core/client"
	"github.com/TrueCloudLab/frostfs-node/pkg/core/netmap"
	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/engine"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object/util"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object_manager/placement"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	"github.com/TrueCloudLab/frostfs-sdk-go/object"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Service utility serving requests of Object.Get service.
type Service struct {
	cfg
}

// Option is a Service's constructor option.
type Option func(*cfg)

type getClient interface {
	getObject(*execCtx, client.NodeInfo) (*object.Object, error)
}

type cfg struct {
	assembly bool

	log *logger.Logger

	localStorage interface {
		get(*execCtx) (*object.Object, error)
	}

	clientCache interface {
		get(client.NodeInfo) (getClient, error)
	}

	traverserGenerator interface {
		GenerateTraverser(cid.ID, *oid.ID, uint64) (*placement.Traverser, error)
	}

	epochSource epochSource

	keyStore *util.KeyStorage
}

type epochSource interface {
	Epoch() (uint64, error)
}

func (c *cfg) initDefault() {
	c.log = &logger.Logger{Logger: zap.L()}
	c.assembly = true
	c.clientCache = new(clientCacheWrapper)
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

// WithLogger returns option to specify Get service's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = &logger.Logger{Logger: l.With(zap.String("component", "Object.Get service"))}
	}
}

// WithoutAssembly returns option to disable object assembling.
func WithoutAssembly() Option {
	return func(c *cfg) {
		c.assembly = false
	}
}

// WithLocalStorageEngine returns option to set local storage
// instance.
func WithLocalStorageEngine(e *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStorage = (*storageEngineWrapper)(e)
	}
}

type ClientConstructor interface {
	Get(client.NodeInfo) (client.MultiAddressClient, error)
}

// WithClientConstructor returns option to set constructor of remote node clients.
func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientCache.(*clientCacheWrapper).cache = v
	}
}

// WithTraverserGenerator returns option to set generator of
// placement traverser to get the objects from containers.
func WithTraverserGenerator(t *util.TraverserGenerator) Option {
	return func(c *cfg) {
		c.traverserGenerator = t
	}
}

// WithNetMapSource returns option to set network
// map storage to receive current network state.
func WithNetMapSource(nmSrc netmap.Source) Option {
	return func(c *cfg) {
		c.epochSource = nmSrc
	}
}

// WithKeyStorage returns option to set private
// key storage for session tokens and node key.
func WithKeyStorage(store *util.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStore = store
	}
}
