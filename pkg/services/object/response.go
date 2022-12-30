package object

import (
	"context"
	"fmt"

	"github.com/TrueCloudLab/frostfs-api-go/v2/object"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util/response"
)

type ResponseService struct {
	respSvc *response.Service

	svc ServiceServer
}

type searchStreamResponser struct {
	SearchStream

	respSvc *response.Service
}

type getStreamResponser struct {
	GetObjectStream

	respSvc *response.Service
}

type getRangeStreamResponser struct {
	GetObjectRangeStream

	respSvc *response.Service
}

type putStreamResponser struct {
	stream  PutObjectStream
	respSvc *response.Service
}

// NewResponseService returns object service instance that passes internal service
// call to response service.
func NewResponseService(objSvc ServiceServer, respSvc *response.Service) *ResponseService {
	return &ResponseService{
		respSvc: respSvc,
		svc:     objSvc,
	}
}

func (s *getStreamResponser) Send(resp *object.GetResponse) error {
	s.respSvc.SetMeta(resp)
	return s.GetObjectStream.Send(resp)
}

func (s *ResponseService) Get(req *object.GetRequest, stream GetObjectStream) error {
	return s.svc.Get(req, &getStreamResponser{
		GetObjectStream: stream,
		respSvc:         s.respSvc,
	})
}

func (s *putStreamResponser) Send(req *object.PutRequest) error {
	if err := s.stream.Send(req); err != nil {
		return fmt.Errorf("could not send the request: %w", err)
	}
	return nil
}

func (s *putStreamResponser) CloseAndRecv() (*object.PutResponse, error) {
	r, err := s.stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("could not close stream and receive response: %w", err)
	}

	s.respSvc.SetMeta(r)
	return r, nil
}

func (s *ResponseService) Put(ctx context.Context) (PutObjectStream, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not create Put object streamer: %w", err)
	}

	return &putStreamResponser{
		stream:  stream,
		respSvc: s.respSvc,
	}, nil
}

func (s *ResponseService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	resp, err := s.svc.Head(ctx, req)
	if err != nil {
		return nil, err
	}

	s.respSvc.SetMeta(resp)
	return resp, nil
}

func (s *searchStreamResponser) Send(resp *object.SearchResponse) error {
	s.respSvc.SetMeta(resp)
	return s.SearchStream.Send(resp)
}

func (s *ResponseService) Search(req *object.SearchRequest, stream SearchStream) error {
	return s.svc.Search(req, &searchStreamResponser{
		SearchStream: stream,
		respSvc:      s.respSvc,
	})
}

func (s *ResponseService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	resp, err := s.svc.Delete(ctx, req)
	if err != nil {
		return nil, err
	}

	s.respSvc.SetMeta(resp)
	return resp, nil
}

func (s *getRangeStreamResponser) Send(resp *object.GetRangeResponse) error {
	s.respSvc.SetMeta(resp)
	return s.GetObjectRangeStream.Send(resp)
}

func (s *ResponseService) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	return s.svc.GetRange(req, &getRangeStreamResponser{
		GetObjectRangeStream: stream,
		respSvc:              s.respSvc,
	})
}

func (s *ResponseService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	resp, err := s.svc.GetRangeHash(ctx, req)
	if err != nil {
		return nil, err
	}

	s.respSvc.SetMeta(resp)
	return resp, nil
}
