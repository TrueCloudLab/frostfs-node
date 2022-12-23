package reputationrpc

import (
	"context"

	"github.com/TrueCloudLab/frostfs-api-go/v2/reputation"
)

// Server is an interface of the NeoFS API v2 Reputation service server.
type Server interface {
	AnnounceLocalTrust(context.Context, *reputation.AnnounceLocalTrustRequest) (*reputation.AnnounceLocalTrustResponse, error)
	AnnounceIntermediateResult(context.Context, *reputation.AnnounceIntermediateResultRequest) (*reputation.AnnounceIntermediateResultResponse, error)
}
