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
	version refs.Version

	state netmap.State
}

// NewService creates, initializes and returns Service instance.
func NewService(nmState netmap.State) *Service {
	s := &Service{state: nmState}
	version.Current().WriteToV2(&s.version)
	return s
}

// SetMeta sets adds meta-header to resp.
func (s *Service) SetMeta(resp util.ResponseMessage) {
	meta := new(session.ResponseMetaHeader)
	meta.SetVersion(&s.version)
	meta.SetTTL(1) // FIXME: #1160 TTL must be calculated
	meta.SetEpoch(s.state.CurrentEpoch())

	if origin := resp.GetMetaHeader(); origin != nil {
		// FIXME: #1160 what if origin is set by local server?
		meta.SetOrigin(origin)
	}

	resp.SetMetaHeader(meta)
}
