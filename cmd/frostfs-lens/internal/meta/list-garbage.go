package meta

import (
	common "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-lens/internal"
	meta "github.com/TrueCloudLab/frostfs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
)

var listGarbageCMD = &cobra.Command{
	Use:   "list-garbage",
	Short: "Garbage listing",
	Long:  `List all the objects that have received GC Mark.`,
	Run:   listGarbageFunc,
}

func init() {
	common.AddComponentPathFlag(listGarbageCMD, &vPath)
}

func listGarbageFunc(cmd *cobra.Command, _ []string) {
	db := openMeta(cmd)
	defer db.Close()

	var garbPrm meta.GarbageIterationPrm
	garbPrm.SetHandler(
		func(garbageObject meta.GarbageObject) error {
			cmd.Println(garbageObject.Address().EncodeToString())
			return nil
		})

	err := db.IterateOverGarbage(garbPrm)
	common.ExitOnErr(cmd, common.Errf("could not iterate over garbage bucket: %w", err))
}
