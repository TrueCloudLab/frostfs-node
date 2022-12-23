package netmap

import (
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	"github.com/spf13/cobra"
)

var getEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Get current epoch number",
	Long:  "Get current epoch number",
	Run: func(cmd *cobra.Command, args []string) {
		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, p, commonflags.RPC)

		var prm internalclient.NetworkInfoPrm
		prm.SetClient(cli)

		res, err := internalclient.NetworkInfo(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		netInfo := res.NetworkInfo()

		cmd.Println(netInfo.CurrentEpoch())
	},
}

func initGetEpochCmd() {
	commonflags.Init(getEpochCmd)
	commonflags.InitAPI(getEpochCmd)
}
