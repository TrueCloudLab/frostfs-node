package session

import (
	"context"
	"fmt"

	"github.com/TrueCloudLab/frostfs-api-go/v2/session"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util/response"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type ServiceExecutor interface {
	Create(context.Context, *session.CreateRequestBody) (*session.CreateResponseBody, error)
}

type executorSvc struct {
	exec ServiceExecutor

	respSvc *response.Service

	log *logger.Logger
}

// NewExecutionService wraps ServiceExecutor and returns Session Service interface.
func NewExecutionService(exec ServiceExecutor, respSvc *response.Service, l *logger.Logger) Server {
	return &executorSvc{
		exec:    exec,
		log:     l,
		respSvc: respSvc,
	}
}

func (s *executorSvc) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	s.log.Debug("serving request...",
		zap.String("component", "SessionService"),
		zap.String("request", "Create"),
	)

	respBody, err := s.exec.Create(ctx, req.GetBody())
	if err != nil {
		return nil, fmt.Errorf("could not execute Create request: %w", err)
	}

	resp := new(session.CreateResponse)
	resp.SetBody(respBody)

	s.respSvc.SetMeta(resp)
	return resp, nil
}
