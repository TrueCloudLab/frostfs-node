package accounting

import (
	"context"

	"github.com/TrueCloudLab/frostfs-api-go/v2/accounting"
	accountingGRPC "github.com/TrueCloudLab/frostfs-api-go/v2/accounting/grpc"
	accountingsvc "github.com/TrueCloudLab/frostfs-node/pkg/services/accounting"
)

// Server wraps FrostFS API Accounting service and
// provides gRPC Accounting service server interface.
type Server struct {
	srv accountingsvc.Server
}

// New creates, initializes and returns Server instance.
func New(c accountingsvc.Server) *Server {
	return &Server{
		srv: c,
	}
}

// Balance converts gRPC BalanceRequest message and passes it to internal Accounting service.
func (s *Server) Balance(ctx context.Context, req *accountingGRPC.BalanceRequest) (*accountingGRPC.BalanceResponse, error) {
	balReq := new(accounting.BalanceRequest)
	if err := balReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Balance(ctx, balReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*accountingGRPC.BalanceResponse), nil
}
