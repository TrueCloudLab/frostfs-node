package netmap

import (
	"context"
	"crypto/ecdsa"

	"github.com/TrueCloudLab/frostfs-api-go/v2/netmap"
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

func (s *signService) LocalNodeInfo(
	ctx context.Context,
	req *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(netmap.LocalNodeInfoResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.LocalNodeInfo(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) NetworkInfo(ctx context.Context, req *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(netmap.NetworkInfoResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.NetworkInfo(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) Snapshot(ctx context.Context, req *netmap.SnapshotRequest) (*netmap.SnapshotResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(netmap.SnapshotResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Snapshot(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}
