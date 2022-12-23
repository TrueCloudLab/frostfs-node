package accounting

import (
	"math/big"

	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/precision"
	"github.com/TrueCloudLab/frostfs-sdk-go/accounting"
	"github.com/TrueCloudLab/frostfs-sdk-go/user"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	ownerFlag = "owner"
)

var accountingBalanceCmd = &cobra.Command{
	Use:   "balance",
	Short: "Get internal balance of NeoFS account",
	Long:  `Get internal balance of NeoFS account`,
	Run: func(cmd *cobra.Command, args []string) {
		var idUser user.ID

		pk := key.GetOrGenerate(cmd)

		balanceOwner, _ := cmd.Flags().GetString(ownerFlag)
		if balanceOwner == "" {
			user.IDFromKey(&idUser, pk.PublicKey)
		} else {
			common.ExitOnErr(cmd, "can't decode owner ID wallet address: %w", idUser.DecodeString(balanceOwner))
		}

		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		var prm internalclient.BalanceOfPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.BalanceOf(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		// print to stdout
		prettyPrintDecimal(cmd, res.Balance())
	},
}

func initAccountingBalanceCmd() {
	ff := accountingBalanceCmd.Flags()

	ff.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	ff.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	ff.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	ff.String(ownerFlag, "", "owner of balance account (omit to use owner from private key)")
}

func prettyPrintDecimal(cmd *cobra.Command, decimal accounting.Decimal) {
	if viper.GetBool(commonflags.Verbose) {
		cmd.Println("value:", decimal.Value())
		cmd.Println("precision:", decimal.Precision())
	} else {
		amountF8 := precision.Convert(decimal.Precision(), 8, big.NewInt(decimal.Value()))

		cmd.Println(fixedn.ToString(amountF8, 8))
	}
}
