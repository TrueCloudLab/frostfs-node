package util

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/TrueCloudLab/frostfs-api-go/v2/session"
	"github.com/TrueCloudLab/frostfs-api-go/v2/signature"
	apistatus "github.com/TrueCloudLab/frostfs-sdk-go/client/status"
)

type RequestMessage interface {
	GetMetaHeader() *session.RequestMetaHeader
}

// ResponseMessage is an interface of FrostFS response message.
type ResponseMessage interface {
	GetMetaHeader() *session.ResponseMetaHeader
	SetMetaHeader(*session.ResponseMetaHeader)
}

type SignService struct {
	key *ecdsa.PrivateKey
}

var ErrAbortStream = errors.New("abort message stream")

func NewUnarySignService(key *ecdsa.PrivateKey) *SignService {
	return &SignService{
		key: key,
	}
}

// SignResponse response with private key via signature.SignServiceMessage.
// The signature error affects the result depending on the protocol version:
//   - if status return is supported, panics since we cannot return the failed status, because it will not be signed.
//   - otherwise, returns error in order to transport it directly.
func (s *SignService) SignResponse(statusSupported bool, resp ResponseMessage, err error) error {
	if err != nil {
		if !statusSupported {
			return err
		}

		setStatusV2(resp, err)
	}

	err = signature.SignServiceMessage(s.key, resp)
	if err != nil {
		return fmt.Errorf("could not sign response: %w", err)
	}

	return nil
}

func (s *SignService) VerifyRequest(req RequestMessage) error {
	if err := signature.VerifyServiceMessage(req); err != nil {
		var sigErr apistatus.SignatureVerification
		sigErr.SetMessage(err.Error())
		return sigErr
	}
	return nil
}

// WrapResponse creates an appropriate response struct if it is nil.
func WrapResponse[T any](resp *T, err error) (*T, error) {
	if resp != nil {
		return resp, err
	}
	return new(T), nil
}

// IsStatusSupported returns true iff request version implies expecting status return.
// This allows us to handle protocol versions <=2.10 (API statuses was introduced in 2.11 only).
func IsStatusSupported(req RequestMessage) bool {
	version := req.GetMetaHeader().GetVersion()

	mjr := version.GetMajor()

	return mjr > 2 || mjr == 2 && version.GetMinor() >= 11
}

func setStatusV2(resp ResponseMessage, err error) {
	// unwrap error
	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(err) {
		err = e
	}

	session.SetStatus(resp, apistatus.ToStatusV2(apistatus.ErrToStatus(err)))
}
