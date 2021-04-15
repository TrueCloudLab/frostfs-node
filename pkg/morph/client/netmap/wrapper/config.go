package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

const (
	maxObjectSizeConfig   = "MaxObjectSize"
	basicIncomeRateConfig = "BasicIncomeRate"
	auditFeeConfig        = "AuditFee"
	epochDurationConfig   = "EpochDuration"
	containerFeeConfig    = "ContainerFee"
	etIterationsConfig    = "EigenTrustIterations"
)

// MaxObjectSize receives max object size configuration
// value through the Netmap contract call.
func (w *Wrapper) MaxObjectSize() (uint64, error) {
	objectSize, err := w.readUInt64Config(maxObjectSizeConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get epoch number", w)
	}

	return objectSize, nil
}

// BasicIncomeRate returns basic income rate configuration value from network
// config in netmap contract.
func (w *Wrapper) BasicIncomeRate() (uint64, error) {
	rate, err := w.readUInt64Config(basicIncomeRateConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get basic income rate", w)
	}

	return rate, nil
}

// AuditFee returns audit fee configuration value from network
// config in netmap contract.
func (w *Wrapper) AuditFee() (uint64, error) {
	fee, err := w.readUInt64Config(auditFeeConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get audit fee", w)
	}

	return fee, nil
}

// EpochDuration returns number of sidechain blocks per one NeoFS epoch.
func (w *Wrapper) EpochDuration() (uint64, error) {
	epochDuration, err := w.readUInt64Config(epochDurationConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get epoch duration", w)
	}

	return epochDuration, nil
}

// ContainerFee returns fee paid by container owner to each alphabet node
// for container registration.
func (w *Wrapper) ContainerFee() (uint64, error) {
	fee, err := w.readUInt64Config(containerFeeConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get container fee", w)
	}

	return fee, nil
}

// EigenTrustIterations returns global configuration value of iteration cycles
// for EigenTrust algorithm per epoch.
func (w *Wrapper) EigenTrustIterations() (uint64, error) {
	iterations, err := w.readUInt64Config(etIterationsConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get eigen trust iterations", w)
	}

	return iterations, nil
}

func (w *Wrapper) readUInt64Config(key string) (uint64, error) {
	args := netmap.ConfigArgs{}
	args.SetKey([]byte(key))

	vals, err := w.client.Config(args, netmap.IntegerAssert)
	if err != nil {
		return 0, err
	}

	v := vals.Value()

	sz, ok := v.(int64)
	if !ok {
		return 0, errors.Errorf("(%T) invalid value type %T", w, v)
	}

	return uint64(sz), nil
}
