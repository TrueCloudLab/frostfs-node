package netmap

import (
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Request current local snapshot of the network map",
	Long:  `Request current local snapshot of the network map`,
	Run: func(cmd *cobra.Command, args []string) {
		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, p, commonflags.RPC)

		var prm internalclient.NetMapSnapshotPrm
		prm.SetClient(cli)

		res, err := internalclient.NetMapSnapshot(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		common.PrettyPrintNetMap(cmd, res.NetMap())
	},
}

func initSnapshotCmd() {
	commonflags.Init(snapshotCmd)
	commonflags.InitAPI(snapshotCmd)
}
