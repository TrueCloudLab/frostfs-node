package reputationrpc

import (
	"context"
	"crypto/ecdsa"

	"github.com/TrueCloudLab/frostfs-api-go/v2/reputation"
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

func (s *signService) AnnounceLocalTrust(ctx context.Context, req *reputation.AnnounceLocalTrustRequest) (*reputation.AnnounceLocalTrustResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(reputation.AnnounceLocalTrustResponse)
		return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
	}
	resp, err := util.WrapResponse(s.svc.AnnounceLocalTrust(ctx, req))
	return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
}

func (s *signService) AnnounceIntermediateResult(ctx context.Context, req *reputation.AnnounceIntermediateResultRequest) (*reputation.AnnounceIntermediateResultResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(reputation.AnnounceIntermediateResultResponse)
		return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
	}
	resp, err := util.WrapResponse(s.svc.AnnounceIntermediateResult(ctx, req))
	return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
}
