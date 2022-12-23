package storagegroup

import (
	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	objectCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/object"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var sgDelCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete storage group from NeoFS",
	Long:  "Delete storage group from NeoFS",
	Run:   delSG,
}

func initSGDeleteCmd() {
	commonflags.Init(sgDelCmd)

	flags := sgDelCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgDelCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringVarP(&sgID, sgIDFlag, "", "", "Storage group identifier")
	_ = sgDelCmd.MarkFlagRequired(sgIDFlag)
}

func delSG(cmd *cobra.Command, _ []string) {
	pk := key.GetOrGenerate(cmd)

	var cnr cid.ID
	var obj oid.ID

	addr := readObjectAddress(cmd, &cnr, &obj)

	var prm internalclient.DeleteObjectPrm
	objectCli.OpenSession(cmd, &prm, pk, cnr, &obj)
	objectCli.Prepare(cmd, &prm)
	prm.SetAddress(addr)

	res, err := internalclient.DeleteObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	tombstone := res.Tombstone()

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", tombstone)
}
