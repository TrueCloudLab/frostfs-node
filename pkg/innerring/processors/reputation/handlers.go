package reputation

import (
	"encoding/hex"

	"github.com/TrueCloudLab/frostfs-node/pkg/morph/event"
	reputationEvent "github.com/TrueCloudLab/frostfs-node/pkg/morph/event/reputation"
	"go.uber.org/zap"
)

func (rp *Processor) handlePutReputation(ev event.Event) {
	put := ev.(reputationEvent.Put)
	peerID := put.PeerID()

	// FIXME: #1147 do not use `ToV2` method outside frostfs-api-go library
	rp.log.Info("notification",
		zap.String("type", "reputation put"),
		zap.String("peer_id", hex.EncodeToString(peerID.PublicKey())))

	// send event to the worker pool

	err := rp.pool.Submit(func() { rp.processPut(&put) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		rp.log.Warn("reputation worker pool drained",
			zap.Int("capacity", rp.pool.Cap()))
	}
}
