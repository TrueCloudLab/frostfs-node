package object

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/TrueCloudLab/frostfs-api-go/v2/object"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/util"
)

type SignService struct {
	key *ecdsa.PrivateKey

	sigSvc *util.SignService

	svc ServiceServer
}

type searchStreamSigner struct {
	SearchStream
	statusSupported bool
	sigSvc          *util.SignService
	nonEmptyResp    bool // set on first Send call
}

type getStreamSigner struct {
	GetObjectStream
	statusSupported bool
	sigSvc          *util.SignService
}

type putStreamSigner struct {
	sigSvc          *util.SignService
	stream          PutObjectStream
	statusSupported bool
	err             error
}

type getRangeStreamSigner struct {
	GetObjectRangeStream
	statusSupported bool
	sigSvc          *util.SignService
}

func NewSignService(key *ecdsa.PrivateKey, svc ServiceServer) *SignService {
	return &SignService{
		key:    key,
		sigSvc: util.NewUnarySignService(key),
		svc:    svc,
	}
}

func (s *getStreamSigner) Send(resp *object.GetResponse) error {
	if err := s.sigSvc.SignResponse(s.statusSupported, resp, nil); err != nil {
		return err
	}
	return s.GetObjectStream.Send(resp)
}

func (s *SignService) Get(req *object.GetRequest, stream GetObjectStream) error {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(object.GetResponse)
		_ = s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
		return stream.Send(resp)
	}

	return s.svc.Get(req, &getStreamSigner{
		GetObjectStream: stream,
		sigSvc:          s.sigSvc,
		statusSupported: util.IsStatusSupported(req),
	})
}

func (s *putStreamSigner) Send(req *object.PutRequest) error {
	s.statusSupported = util.IsStatusSupported(req)

	if s.err = s.sigSvc.VerifyRequest(req); s.err != nil {
		return util.ErrAbortStream
	}
	if s.err = s.stream.Send(req); s.err != nil {
		return util.ErrAbortStream
	}
	return nil
}

func (s *putStreamSigner) CloseAndRecv() (resp *object.PutResponse, err error) {
	if s.err != nil {
		err = s.err
		resp = new(object.PutResponse)
	} else {
		resp, err = s.stream.CloseAndRecv()
		if err != nil {
			return nil, fmt.Errorf("could not close stream and receive response: %w", err)
		}
	}

	return resp, s.sigSvc.SignResponse(s.statusSupported, resp, err)
}

func (s *SignService) Put(ctx context.Context) (PutObjectStream, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not create Put object streamer: %w", err)
	}

	return &putStreamSigner{
		stream: stream,
		sigSvc: s.sigSvc,
	}, nil
}

func (s *SignService) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(object.HeadResponse)
		return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Head(ctx, req))
	return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
}

func (s *searchStreamSigner) Send(resp *object.SearchResponse) error {
	s.nonEmptyResp = true
	if err := s.sigSvc.SignResponse(s.statusSupported, resp, nil); err != nil {
		return err
	}
	return s.SearchStream.Send(resp)
}

func (s *SignService) Search(req *object.SearchRequest, stream SearchStream) error {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(object.SearchResponse)
		_ = s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
		return stream.Send(resp)
	}

	ss := &searchStreamSigner{
		SearchStream:    stream,
		sigSvc:          s.sigSvc,
		statusSupported: util.IsStatusSupported(req),
	}
	err := s.svc.Search(req, ss)
	if err == nil && !ss.nonEmptyResp {
		// The higher component does not write any response in the case of an empty result (which is correct).
		// With the introduction of status returns at least one answer must be signed and sent to the client.
		// This approach is supported by clients who do not know how to work with statuses (one could make
		// a switch according to the protocol version from the request, but the costs of sending an empty
		// answer can be neglected due to the gradual refusal to use the "old" clients).
		return stream.Send(new(object.SearchResponse))
	}
	return err
}

func (s *SignService) Delete(ctx context.Context, req *object.DeleteRequest) (*object.DeleteResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(object.DeleteResponse)
		return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
	}
	resp, err := util.WrapResponse(s.svc.Delete(ctx, req))
	return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
}

func (s *getRangeStreamSigner) Send(resp *object.GetRangeResponse) error {
	if err := s.sigSvc.SignResponse(s.statusSupported, resp, nil); err != nil {
		return err
	}
	return s.GetObjectRangeStream.Send(resp)
}

func (s *SignService) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(object.GetRangeResponse)
		_ = s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
		return stream.Send(resp)
	}

	return s.svc.GetRange(req, &getRangeStreamSigner{
		GetObjectRangeStream: stream,
		sigSvc:               s.sigSvc,
		statusSupported:      util.IsStatusSupported(req),
	})
}

func (s *SignService) GetRangeHash(ctx context.Context, req *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	if err := s.sigSvc.VerifyRequest(req); err != nil {
		resp := new(object.GetRangeHashResponse)
		return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
	}
	resp, err := util.WrapResponse(s.svc.GetRangeHash(ctx, req))
	return resp, s.sigSvc.SignResponse(util.IsStatusSupported(req), resp, err)
}
