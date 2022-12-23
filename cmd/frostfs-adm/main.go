package main

import (
	"os"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-adm/internal/modules"
)

func main() {
	if err := modules.Execute(); err != nil {
		os.Exit(1)
	}
}
