package main

import (
	nodeconfig "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/node"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/attributes"
)

func parseAttributes(c *cfg) {
	if nodeconfig.Relay(c.appCfg) {
		return
	}

	fatalOnErr(attributes.ReadNodeAttributes(&c.cfgNodeInfo.localInfo, nodeconfig.Attributes(c.appCfg)))
}
