package cmd

import (
	"github.com/TrueCloudLab/frostfs-node/pkg/util/autocomplete"
)

func init() {
	rootCmd.AddCommand(autocomplete.Command("frostfs-cli"))
}
