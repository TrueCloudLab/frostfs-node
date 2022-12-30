package accounting

import (
	"context"
	"crypto/ecdsa"

	"github.com/TrueCloudLab/frostfs-api-go/v2/accounting"
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

func (s *signService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	resp, err := util.WrapResponse(s.svc.Balance(ctx, req))
	return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
}
