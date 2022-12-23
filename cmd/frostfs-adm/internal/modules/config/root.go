package config

import (
	"github.com/spf13/cobra"
)

const configPathFlag = "path"

var (
	// RootCmd is a root command of config section.
	RootCmd = &cobra.Command{
		Use:   "config",
		Short: "Section for frostfs-adm config related commands",
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize basic frostfs-adm configuration file",
		Example: `frostfs-adm config init
frostfs-adm config init --path .config/frostfs-adm.yml`,
		RunE: initConfig,
	}
)

func init() {
	RootCmd.AddCommand(initCmd)

	initCmd.Flags().String(configPathFlag, "", "Path to config (default ~/.frostfs/adm/config.yml)")
}
