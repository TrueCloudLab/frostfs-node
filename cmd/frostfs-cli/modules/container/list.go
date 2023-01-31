package container

import (
	"strings"

	"github.com/TrueCloudLab/frostfs-api-go/v2/container"
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/TrueCloudLab/frostfs-sdk-go/user"
	"github.com/spf13/cobra"
)

// flags of list command.
const (
	flagListPrintAttr      = "with-attr"
	flagListContainerOwner = "owner"
)

// flag vars of list command.
var (
	flagVarListPrintAttr      bool
	flagVarListContainerOwner string
)

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Run: func(cmd *cobra.Command, args []string) {
		var idUser user.ID

		key := key.GetOrGenerate(cmd)

		if flagVarListContainerOwner == "" {
			user.IDFromKey(&idUser, key.PublicKey)
		} else {
			err := idUser.DecodeString(flagVarListContainerOwner)
			commonCmd.ExitOnErr(cmd, "invalid user ID: %w", err)
		}

		cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

		var prm internalclient.ListContainersPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.ListContainers(prm)
		commonCmd.ExitOnErr(cmd, "rpc error: %w", err)

		var prmGet internalclient.GetContainerPrm
		prmGet.SetClient(cli)

		list := res.IDList()
		for i := range list {
			cmd.Println(list[i].String())

			if flagVarListPrintAttr {
				prmGet.SetContainer(list[i])

				res, err := internalclient.GetContainer(prmGet)
				if err == nil {
					res.Container().IterateAttributes(func(key, val string) {
						if !strings.HasPrefix(key, container.SysAttributePrefix) {
							// FIXME(@cthulhu-rider): neofs-sdk-go#314 use dedicated method to skip system attributes
							cmd.Printf("  %s: %s\n", key, val)
						}
					})
				} else {
					cmd.Printf("  failed to read attributes: %v\n", err)
				}
			}
		}
	},
}

func initContainerListContainersCmd() {
	commonflags.Init(listContainersCmd)

	flags := listContainersCmd.Flags()

	flags.StringVar(&flagVarListContainerOwner, flagListContainerOwner, "",
		"Owner of containers (omit to use owner from private key)",
	)
	flags.BoolVar(&flagVarListPrintAttr, flagListPrintAttr, false,
		"Request and print attributes of each container",
	)
}
