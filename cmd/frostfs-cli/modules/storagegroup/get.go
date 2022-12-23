package storagegroup

import (
	"bytes"

	internalclient "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/client"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/key"
	objectCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/object"
	cid "github.com/TrueCloudLab/frostfs-sdk-go/container/id"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	storagegroupSDK "github.com/TrueCloudLab/frostfs-sdk-go/storagegroup"
	"github.com/spf13/cobra"
)

var sgID string

var sgGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get storage group from NeoFS",
	Long:  "Get storage group from NeoFS",
	Run:   getSG,
}

func initSGGetCmd() {
	commonflags.Init(sgGetCmd)

	flags := sgGetCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgGetCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringVarP(&sgID, sgIDFlag, "", "", "Storage group identifier")
	_ = sgGetCmd.MarkFlagRequired(sgIDFlag)

	flags.Bool(sgRawFlag, false, "Set raw request option")
}

func getSG(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	var obj oid.ID

	addr := readObjectAddress(cmd, &cnr, &obj)
	pk := key.GetOrGenerate(cmd)
	buf := bytes.NewBuffer(nil)

	cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

	var prm internalclient.GetObjectPrm
	objectCli.Prepare(cmd, &prm)
	prm.SetClient(cli)

	raw, _ := cmd.Flags().GetBool(sgRawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(addr)
	prm.SetPayloadWriter(buf)

	res, err := internalclient.GetObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	rawObj := res.Header()
	rawObj.SetPayload(buf.Bytes())

	var sg storagegroupSDK.StorageGroup

	err = storagegroupSDK.ReadFromObject(&sg, *rawObj)
	common.ExitOnErr(cmd, "could not read storage group from the obj: %w", err)

	cmd.Printf("Expiration epoch: %d\n", sg.ExpirationEpoch())
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	common.PrintChecksum(cmd, "Group hash", sg.ValidationDataHash)

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i].String())
		}
	}
}
