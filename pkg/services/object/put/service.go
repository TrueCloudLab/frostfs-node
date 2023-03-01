package putsvc

import (
	"context"

	"github.com/TrueCloudLab/frostfs-node/pkg/core/client"
	"github.com/TrueCloudLab/frostfs-node/pkg/core/container"
	"github.com/TrueCloudLab/frostfs-node/pkg/core/netmap"
	"github.com/TrueCloudLab/frostfs-node/pkg/core/object"
	objutil "github.com/TrueCloudLab/frostfs-node/pkg/services/object/util"
	"github.com/TrueCloudLab/frostfs-node/pkg/util"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type MaxSizeSource interface {
	// MaxObjectSize returns maximum payload size
	// of physically stored object in system.
	//
	// Must return 0 if value can not be obtained.
	MaxObjectSize() uint64
}

type Service struct {
	cfg
}

type Option func(*cfg)

type ClientConstructor interface {
	Get(client.NodeInfo) (client.MultiAddressClient, error)
}

type cfg struct {
	keyStorage *objutil.KeyStorage

	maxSizeSrc MaxSizeSource

	localStore ObjectStorage

	cnrSrc container.Source

	netMapSrc netmap.Source

	remotePool, localPool util.WorkerPool

	netmapKeys netmap.AnnouncedKeys

	fmtValidator *object.FormatValidator

	fmtValidatorOpts []object.FormatValidatorOption

	networkState netmap.State

	clientConstructor ClientConstructor

	log *logger.Logger
}

func (c *cfg) initDefault() {
	c.remotePool = util.NewPseudoWorkerPool()
	c.localPool = util.NewPseudoWorkerPool()
	c.log = &logger.Logger{Logger: zap.L()}
}

func NewService(opts ...Option) *Service {
	var s Service
	s.cfg.initDefault()

	for i := range opts {
		opts[i](&s.cfg)
	}

	s.cfg.fmtValidator = object.NewFormatValidator(s.cfg.fmtValidatorOpts...)

	return &s
}

func (p *Service) Put(ctx context.Context) (*Streamer, error) {
	return &Streamer{
		cfg: &p.cfg,
		ctx: ctx,
	}, nil
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = v
	}
}

func WithMaxSizeSource(v MaxSizeSource) Option {
	return func(c *cfg) {
		c.maxSizeSrc = v
	}
}

func WithObjectStorage(v ObjectStorage) Option {
	return func(c *cfg) {
		c.localStore = v
	}
}

func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

func WithNetworkMapSource(v netmap.Source) Option {
	return func(c *cfg) {
		c.netMapSrc = v
	}
}

func WithWorkerPools(remote, local util.WorkerPool) Option {
	return func(c *cfg) {
		c.remotePool, c.localPool = remote, local
	}
}

func WithNetmapKeys(v netmap.AnnouncedKeys) Option {
	return func(c *cfg) {
		c.netmapKeys = v
	}
}

func WithNetworkState(v netmap.State) Option {
	return func(c *cfg) {
		c.networkState = v
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, object.WithNetState(v))
	}
}

func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientConstructor = v
	}
}

func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}
