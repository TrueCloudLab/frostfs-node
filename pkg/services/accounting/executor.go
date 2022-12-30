package accounting

import (
	"context"
	"fmt"

	"github.com/TrueCloudLab/frostfs-api-go/v2/accounting"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util/response"
)

type ServiceExecutor interface {
	Balance(context.Context, *accounting.BalanceRequestBody) (*accounting.BalanceResponseBody, error)
}

type executorSvc struct {
	exec    ServiceExecutor
	respSvc *response.Service
}

// NewExecutionService wraps ServiceExecutor and returns Accounting Service interface.
func NewExecutionService(exec ServiceExecutor, respSvc *response.Service) Server {
	return &executorSvc{
		exec:    exec,
		respSvc: respSvc,
	}
}

func (s *executorSvc) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	respBody, err := s.exec.Balance(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Balance request: %w", err)
	}

	resp := new(accounting.BalanceResponse)
	resp.SetBody(respBody)

	s.respSvc.SetMeta(resp)
	return resp, nil
}
