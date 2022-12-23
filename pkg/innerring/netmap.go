package innerring

import (
	"fmt"

	"github.com/TrueCloudLab/frostfs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	netmapclient "github.com/TrueCloudLab/frostfs-node/pkg/morph/client/netmap"
)

/*
File contains dependencies for processor of the Netmap contract's notifications.
*/

// wraps Netmap contract's client and provides state.NetworkSettings.
type networkSettings netmapclient.Client

// MaintenanceModeAllowed requests network configuration from the Sidechain
// and check allowance of storage node's maintenance mode according to it.
// Always returns state.ErrMaintenanceModeDisallowed.
func (s *networkSettings) MaintenanceModeAllowed() error {
	allowed, err := (*netmapclient.Client)(s).MaintenanceModeAllowed()
	if err != nil {
		return fmt.Errorf("read maintenance mode's allowance from the Sidechain: %w", err)
	} else if allowed {
		return nil
	}

	return state.ErrMaintenanceModeDisallowed
}
