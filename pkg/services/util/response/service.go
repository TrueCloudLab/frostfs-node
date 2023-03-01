package response

import (
	"github.com/TrueCloudLab/frostfs-api-go/v2/refs"
	"github.com/TrueCloudLab/frostfs-api-go/v2/session"
	"github.com/TrueCloudLab/frostfs-node/pkg/core/netmap"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util"
	"github.com/TrueCloudLab/frostfs-sdk-go/version"
)

// Service represents universal v2 service
// that sets response meta header values.
type Service struct {
	cfg *cfg
}

// Option is an option of Service constructor.
type Option func(*cfg)

type cfg struct {
	version refs.Version

	state netmap.State
}

func defaultCfg() *cfg {
	var c cfg

	version.Current().WriteToV2(&c.version)

	return &c
}

// NewService creates, initializes and returns Service instance.
func NewService(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

// SetMeta sets adds meta-header to resp.
func (s *Service) SetMeta(resp util.ResponseMessage) {
	meta := new(session.ResponseMetaHeader)
	meta.SetVersion(&s.cfg.version)
	meta.SetTTL(1) // FIXME: #1160 TTL must be calculated
	meta.SetEpoch(s.cfg.state.CurrentEpoch())

	if origin := resp.GetMetaHeader(); origin != nil {
		// FIXME: #1160 what if origin is set by local server?
		meta.SetOrigin(origin)
	}

	resp.SetMetaHeader(meta)
}

// WithNetworkState returns option to set network state of Service.
func WithNetworkState(v netmap.State) Option {
	return func(c *cfg) {
		c.state = v
	}
}
