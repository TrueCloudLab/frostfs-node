package netmap

import (
	"github.com/TrueCloudLab/frostfs-node/pkg/morph/client"
	"github.com/TrueCloudLab/frostfs-sdk-go/netmap"
)

// GetNetMap calls "snapshot" method and decodes netmap.NetMap from the response.
func (c *Client) GetNetMap(diff uint64) (*netmap.NetMap, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(snapshotMethod)
	prm.SetArgs(diff)

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, err
	}

	return decodeNetMap(res)
}
