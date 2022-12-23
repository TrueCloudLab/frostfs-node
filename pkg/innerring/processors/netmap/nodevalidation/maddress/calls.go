package maddress

import (
	"fmt"

	"github.com/TrueCloudLab/frostfs-node/pkg/network"
	"github.com/TrueCloudLab/frostfs-sdk-go/netmap"
)

// VerifyAndUpdate calls network.VerifyAddress.
func (v *Validator) VerifyAndUpdate(n *netmap.NodeInfo) error {
	err := network.VerifyMultiAddress(*n)
	if err != nil {
		return fmt.Errorf("could not verify multiaddress: %w", err)
	}

	return nil
}
