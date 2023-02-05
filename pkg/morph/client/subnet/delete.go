package morphsubnet

import (
	"github.com/TrueCloudLab/frostfs-node/pkg/morph/client"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// DeletePrm groups parameters of Delete method of Subnet contract.
type DeletePrm struct {
	cliPrm client.InvokePrm

	args [1]interface{}
}

// SetTxHash sets hash of the transaction which spawned the notification.
// Ignore this parameter for new requests.
func (x *DeletePrm) SetTxHash(hash util.Uint256) {
	x.cliPrm.SetHash(hash)
}

// SetID sets identifier of the subnet to be removed in a binary FrostFS API protocol format.
func (x *DeletePrm) SetID(id []byte) {
	x.args[0] = id
}

// DeleteRes groups the resulting values of Delete method of Subnet contract.
type DeleteRes struct{}

// Delete removes subnet though the call of the corresponding method of the Subnet contract.
func (x Client) Delete(prm DeletePrm) (*DeleteRes, error) {
	prm.cliPrm.SetMethod(deleteMethod)
	prm.cliPrm.SetArgs(prm.args[:]...)

	err := x.client.Invoke(prm.cliPrm)
	if err != nil {
		return nil, err
	}

	return new(DeleteRes), nil
}
