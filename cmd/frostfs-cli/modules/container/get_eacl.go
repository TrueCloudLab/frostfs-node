package container

import (
	"os"

	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/spf13/cobra"
)

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL table of container`,
	Run: func(cmd *cobra.Command, args []string) {
		id := parseContainerID(cmd)
		pk := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		var eaclPrm internalclient.EACLPrm
		eaclPrm.SetClient(cli)
		eaclPrm.SetContainer(id)

		res, err := internalclient.EACL(eaclPrm)
		commonCmd.ExitOnErr(cmd, "rpc error: %w", err)

		eaclTable := res.EACL()

		if containerPathTo == "" {
			cmd.Println("eACL: ")
			common.PrettyPrintJSON(cmd, &eaclTable, "eACL")

			return
		}

		var data []byte

		if containerJSON {
			data, err = eaclTable.MarshalJSON()
			commonCmd.ExitOnErr(cmd, "can't encode to JSON: %w", err)
		} else {
			data, err = eaclTable.Marshal()
			commonCmd.ExitOnErr(cmd, "can't encode to binary: %w", err)
		}

		cmd.Println("dumping data to file:", containerPathTo)

		err = os.WriteFile(containerPathTo, data, 0644)
		commonCmd.ExitOnErr(cmd, "could not write eACL to file: %w", err)
	},
}

func initContainerGetEACLCmd() {
	commonflags.Init(getExtendedACLCmd)

	flags := getExtendedACLCmd.Flags()

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathTo, "to", "", "Path to dump encoded container (default: binary encoded)")
	flags.BoolVar(&containerJSON, commonflags.JSON, false, "Encode EACL table in json format")
}
