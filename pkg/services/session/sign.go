package session

import (
	"context"
	"crypto/ecdsa"

	"github.com/TrueCloudLab/frostfs-api-go/v2/session"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util"
)

type signService struct {
	sigSvc *util.SignService

	svc Server
}

func NewSignService(key *ecdsa.PrivateKey, svc Server) Server {
	return &signService{
		sigSvc: util.NewUnarySignService(key),
		svc:    svc,
	}
}

func (s *signService) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(session.CreateResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Create(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}
