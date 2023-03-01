package deletesvc

import (
	"context"

	objectV2 "github.com/TrueCloudLab/frostfs-api-go/v2/object"
	deletesvc "github.com/TrueCloudLab/frostfs-node/pkg/services/object/delete"
)

// Service implements Delete operation of Object service v2.
type Service struct {
	cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *deletesvc.Service
}

// NewService constructs Service instance from provided options.
func NewService(opts ...Option) *Service {
	var s Service

	for i := range opts {
		opts[i](&s.cfg)
	}

	return &s
}

// Delete calls internal service.
func (s *Service) Delete(ctx context.Context, req *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	resp := new(objectV2.DeleteResponse)

	body := new(objectV2.DeleteResponseBody)
	resp.SetBody(body)

	p, err := s.toPrm(req, body)
	if err != nil {
		return nil, err
	}

	err = s.svc.Delete(ctx, *p)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func WithInternalService(v *deletesvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
