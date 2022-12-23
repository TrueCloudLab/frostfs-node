package frostfs

import (
	"encoding/hex"

	"github.com/TrueCloudLab/frostfs-node/pkg/morph/event"
	frostfsEvent "github.com/TrueCloudLab/frostfs-node/pkg/morph/event/neofs"
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"go.uber.org/zap"
)

func (np *Processor) handleDeposit(ev event.Event) {
	deposit := ev.(frostfsEvent.Deposit)
	np.log.Info("notification",
		zap.String("type", "deposit"),
		zap.String("id", hex.EncodeToString(slice.CopyReverse(deposit.ID()))))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processDeposit(&deposit) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("frostfs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleWithdraw(ev event.Event) {
	withdraw := ev.(frostfsEvent.Withdraw)
	np.log.Info("notification",
		zap.String("type", "withdraw"),
		zap.String("id", hex.EncodeToString(slice.CopyReverse(withdraw.ID()))))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processWithdraw(&withdraw) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("frostfs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleCheque(ev event.Event) {
	cheque := ev.(frostfsEvent.Cheque)
	np.log.Info("notification",
		zap.String("type", "cheque"),
		zap.String("id", hex.EncodeToString(cheque.ID())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processCheque(&cheque) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("frostfs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleConfig(ev event.Event) {
	cfg := ev.(frostfsEvent.Config)
	np.log.Info("notification",
		zap.String("type", "set config"),
		zap.String("key", hex.EncodeToString(cfg.Key())),
		zap.String("value", hex.EncodeToString(cfg.Value())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processConfig(&cfg) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("frostfs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleBind(ev event.Event) {
	e := ev.(frostfsEvent.Bind)
	np.log.Info("notification",
		zap.String("type", "bind"),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processBind(e) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("frostfs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleUnbind(ev event.Event) {
	e := ev.(frostfsEvent.Unbind)
	np.log.Info("notification",
		zap.String("type", "unbind"),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processBind(e) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("frostfs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}
