package blobovnicza

import (
	"fmt"
	"io"

	common "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-lens/internal"
	"github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/blobovnicza"
	oid "github.com/TrueCloudLab/frostfs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var listCMD = &cobra.Command{
	Use:   "list",
	Short: "Object listing",
	Long:  `List all objects stored in a blobovnicza.`,
	Run:   listFunc,
}

func init() {
	common.AddComponentPathFlag(listCMD, &vPath)
}

func listFunc(cmd *cobra.Command, _ []string) {
	// other targets can be supported
	w := cmd.OutOrStderr()

	wAddr := func(addr oid.Address) error {
		_, err := io.WriteString(w, fmt.Sprintf("%s\n", addr))
		return err
	}

	blz := openBlobovnicza(cmd)
	defer blz.Close()

	err := blobovnicza.IterateAddresses(blz, wAddr)
	common.ExitOnErr(cmd, common.Errf("blobovnicza iterator failure: %w", err))
}
