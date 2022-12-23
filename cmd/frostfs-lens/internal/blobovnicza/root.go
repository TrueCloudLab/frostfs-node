package blobovnicza

import (
	common "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-lens/internal"
	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/blobovnicza"
	"github.com/spf13/cobra"
)

var (
	vAddress string
	vPath    string
	vOut     string
)

// Root contains `blobovnicza` command definition.
var Root = &cobra.Command{
	Use:   "blobovnicza",
	Short: "Operations with a blobovnicza",
}

func init() {
	Root.AddCommand(listCMD, inspectCMD)
}

func openBlobovnicza(cmd *cobra.Command) *blobovnicza.Blobovnicza {
	blz := blobovnicza.New(
		blobovnicza.WithPath(vPath),
		blobovnicza.WithReadOnly(true),
	)
	common.ExitOnErr(cmd, blz.Open())

	return blz
}
