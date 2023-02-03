package netmap

import (
	"encoding/hex"
	"time"

	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/spf13/cobra"
)

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about FrostFS network",
	Long:  "Get information about FrostFS network",
	Run: func(cmd *cobra.Command, args []string) {
		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, p, commonflags.RPC)

		var prm internalclient.NetworkInfoPrm
		prm.SetClient(cli)

		res, err := internalclient.NetworkInfo(prm)
		commonCmd.ExitOnErr(cmd, "rpc error: %w", err)

		netInfo := res.NetworkInfo()

		cmd.Printf("Epoch: %d\n", netInfo.CurrentEpoch())

		magic := netInfo.MagicNumber()
		cmd.Printf("Network magic: [%s] %d\n", netmode.Magic(magic), magic)

		cmd.Printf("Time per block: %s\n", time.Duration(netInfo.MsPerBlock())*time.Millisecond)

		const format = "  %s: %v\n"

		cmd.Println("FrostFS network configuration (system)")
		cmd.Printf(format, "Audit fee", netInfo.AuditFee())
		cmd.Printf(format, "Storage price", netInfo.StoragePrice())
		cmd.Printf(format, "Container fee", netInfo.ContainerFee())
		cmd.Printf(format, "EigenTrust alpha", netInfo.EigenTrustAlpha())
		cmd.Printf(format, "Number of EigenTrust iterations", netInfo.NumberOfEigenTrustIterations())
		cmd.Printf(format, "Epoch duration", netInfo.EpochDuration())
		cmd.Printf(format, "Inner Ring candidate fee", netInfo.IRCandidateFee())
		cmd.Printf(format, "Maximum object size", netInfo.MaxObjectSize())
		cmd.Printf(format, "Withdrawal fee", netInfo.WithdrawalFee())
		cmd.Printf(format, "Homomorphic hashing disabled", netInfo.HomomorphicHashingDisabled())
		cmd.Printf(format, "Maintenance mode allowed", netInfo.MaintenanceModeAllowed())

		cmd.Println("FrostFS network configuration (other)")
		netInfo.IterateRawNetworkParameters(func(name string, value []byte) {
			cmd.Printf(format, name, hex.EncodeToString(value))
		})
	},
}

func initNetInfoCmd() {
	commonflags.Init(netInfoCmd)
	commonflags.InitAPI(netInfoCmd)
}
