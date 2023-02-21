package session

import (
	"context"

	"github.com/TrueCloudLab/frostfs-api-go/v2/session"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc Server
}

// NewResponseService returns session service instance that passes internal service
// call to response service.
func NewResponseService(ssSvc Server, respSvc *response.Service) Server {
	return &responseService{
		respSvc: respSvc,
		svc:     ssSvc,
	}
}

func (s *responseService) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.Create(ctx, req.(*session.CreateRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*session.CreateResponse), nil
}
