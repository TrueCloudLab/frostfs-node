package storagegroup

import (
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	objectCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/object"
	"github.com/TrueCloudLab/frostfs-node/pkg/services/object_manager/storagegroup"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

var sgListCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage groups in NeoFS container",
	Long:  "List storage groups in NeoFS container",
	Run:   listSG,
}

func initSGListCmd() {
	commonflags.Init(sgListCmd)

	sgListCmd.Flags().String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgListCmd.MarkFlagRequired(commonflags.CIDFlag)
}

func listSG(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	readCID(cmd, &cnr)

	pk := key.GetOrGenerate(cmd)

	cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

	var prm internalclient.SearchObjectsPrm
	objectCli.Prepare(cmd, &prm)
	prm.SetClient(cli)
	prm.SetContainerID(cnr)
	prm.SetFilters(storagegroup.SearchQuery())

	res, err := internalclient.SearchObjects(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	ids := res.IDList()

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for i := range ids {
		cmd.Println(ids[i].String())
	}
}
