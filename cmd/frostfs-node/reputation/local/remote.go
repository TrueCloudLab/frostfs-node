package local

import (
	"crypto/ecdsa"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/reputation/common"
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/reputation/internal/client"
	coreclient "github.com/TrueCloudLab/frostfs-node/pkg/core/client"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/reputation"
	reputationcommon "github.com/TrueCloudLab/frostfs-node/pkg/services/reputation/common"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/logger"
	reputationapi "github.com/TrueCloudLab/frostfs-sdk-go/reputation"
	"go.uber.org/zap"
)

// RemoteProviderPrm groups the required parameters of the RemoteProvider's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type RemoteProviderPrm struct {
	Key *ecdsa.PrivateKey
	Log *logger.Logger
}

// NewRemoteProvider creates a new instance of the RemoteProvider.
//
// Panics if at least one value of the parameters is invalid.
//
// The created RemoteProvider does not require additional
// initialization and is completely ready for work.
func NewRemoteProvider(prm RemoteProviderPrm) *RemoteProvider {
	switch {
	case prm.Key == nil:
		common.PanicOnPrmValue("NetMapSource", prm.Key)
	case prm.Log == nil:
		common.PanicOnPrmValue("Logger", prm.Log)
	}

	return &RemoteProvider{
		key: prm.Key,
		log: prm.Log,
	}
}

// RemoteProvider is an implementation of the clientKeyRemoteProvider interface.
type RemoteProvider struct {
	key *ecdsa.PrivateKey
	log *logger.Logger
}

func (rp RemoteProvider) WithClient(c coreclient.Client) reputationcommon.WriterProvider {
	return &TrustWriterProvider{
		client: c,
		key:    rp.key,
		log:    rp.log,
	}
}

type TrustWriterProvider struct {
	client coreclient.Client
	key    *ecdsa.PrivateKey
	log    *logger.Logger
}

func (twp *TrustWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &RemoteTrustWriter{
		ctx:    ctx,
		client: twp.client,
		key:    twp.key,
		log:    twp.log,
	}, nil
}

type RemoteTrustWriter struct {
	ctx    reputationcommon.Context
	client coreclient.Client
	key    *ecdsa.PrivateKey
	log    *logger.Logger

	buf []reputationapi.Trust
}

func (rtp *RemoteTrustWriter) Write(t reputation.Trust) error {
	var apiTrust reputationapi.Trust

	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(t.Peer())

	rtp.buf = append(rtp.buf, apiTrust)

	return nil
}

func (rtp *RemoteTrustWriter) Close() error {
	epoch := rtp.ctx.Epoch()

	rtp.log.Debug("announcing trusts",
		zap.Uint64("epoch", epoch),
	)

	var prm internalclient.AnnounceLocalPrm

	prm.SetContext(rtp.ctx)
	prm.SetClient(rtp.client)
	prm.SetEpoch(epoch)
	prm.SetTrusts(rtp.buf)

	_, err := internalclient.AnnounceLocal(prm)

	return err
}
