package container

import (
	"context"
	"crypto/ecdsa"

	"github.com/TrueCloudLab/frostfs-api-go/v2/container"
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

func (s *signService) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.PutResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Put(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.DeleteResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Delete(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.GetResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Get(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.ListResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.List(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.SetExtendedACLResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.SetExtendedACL(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.GetExtendedACLResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.GetExtendedACL(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}

func (s *signService) AnnounceUsedSpace(ctx context.Context, req *container.AnnounceUsedSpaceRequest) (*container.AnnounceUsedSpaceResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(container.AnnounceUsedSpaceResponse)
		return resp, s.sigSvc.SignResponse(req, resp, err)
	}
	resp, err := util.WrapResponse(s.svc.AnnounceUsedSpace(ctx, req))
	return resp, s.sigSvc.SignResponse(req, resp, err)
}
