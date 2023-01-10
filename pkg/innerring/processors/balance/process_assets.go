package balance

import (
	frostfsContract "github.com/TrueCloudLab/frostfs-node/pkg/morph/client/frostfs"
	balanceEvent "github.com/TrueCloudLab/frostfs-node/pkg/morph/event/balance"
	"go.uber.org/zap"
)

// Process lock event by invoking Cheque method in main net to send assets
// back to the withdraw issuer.
func (bp *Processor) processLock(lock *balanceEvent.Lock) {
	if !bp.alphabetState.IsAlphabet() {
		bp.log.Info("non alphabet mode, ignore balance lock")
		return
	}

	prm := frostfsContract.ChequePrm{}

	prm.SetID(lock.ID())
	prm.SetUser(lock.User())
	prm.SetAmount(bp.converter.ToFixed8(lock.Amount()))
	prm.SetLock(lock.LockAccount())
	prm.SetHash(lock.TxHash())

	err := bp.frostfsClient.Cheque(prm)
	if err != nil {
		bp.log.Error("can't send lock asset tx", zap.Error(err))
	}
}
