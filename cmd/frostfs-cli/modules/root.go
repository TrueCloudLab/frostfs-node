package cmd

import (
	"os"
	"path/filepath"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/common"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/internal/commonflags"
	accountingCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/accounting"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/acl"
	bearerCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/bearer"
	containerCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/container"
	controlCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/control"
	netmapCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/netmap"
	objectCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/object"
	sessionCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/session"
	sgCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/storagegroup"
	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/tree"
	utilCli "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-cli/modules/util"
	commonCmd "github.com/TrueCloudLab/frostfs-node/cmd/internal/common"
	"github.com/TrueCloudLab/frostfs-node/misc"
	"github.com/TrueCloudLab/frostfs-node/pkg/util/gendoc"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	envPrefix = "FROSTFS_CLI"
)

// Global scope flags.
var (
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "frostfs-cli",
	Short: "Command Line Tool to work with FrostFS",
	Long: `FrostFS CLI provides all basic interactions with FrostFS and it's services.

It contains commands for interaction with FrostFS nodes using different versions
of frostfs-api and some useful utilities for compiling ACL rules from JSON
notation, managing container access through protocol gates, querying network map
and much more!`,
	Run: entryPoint,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	commonCmd.ExitOnErr(rootCmd, "", err)
}

func init() {
	cobra.OnInitialize(initConfig)

	// use stdout as default output for cmd.Print()
	rootCmd.SetOut(os.Stdout)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Config file (default is $HOME/.config/frostfs-cli/config.yaml)")
	rootCmd.PersistentFlags().BoolP(commonflags.Verbose, commonflags.VerboseShorthand,
		false, commonflags.VerboseUsage)

	_ = viper.BindPFlag(commonflags.Verbose, rootCmd.PersistentFlags().Lookup(commonflags.Verbose))

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().Bool("version", false, "Application version and FrostFS API compatibility")

	rootCmd.AddCommand(acl.Cmd)
	rootCmd.AddCommand(bearerCli.Cmd)
	rootCmd.AddCommand(sessionCli.Cmd)
	rootCmd.AddCommand(accountingCli.Cmd)
	rootCmd.AddCommand(controlCli.Cmd)
	rootCmd.AddCommand(utilCli.Cmd)
	rootCmd.AddCommand(netmapCli.Cmd)
	rootCmd.AddCommand(objectCli.Cmd)
	rootCmd.AddCommand(sgCli.Cmd)
	rootCmd.AddCommand(containerCli.Cmd)
	rootCmd.AddCommand(tree.Cmd)
	rootCmd.AddCommand(gendoc.Command(rootCmd))
}

func entryPoint(cmd *cobra.Command, _ []string) {
	printVersion, _ := cmd.Flags().GetBool("version")
	if printVersion {
		cmd.Print(misc.BuildInfo("FrostFS CLI"))

		return
	}

	_ = cmd.Usage()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		commonCmd.ExitOnErr(rootCmd, "", err)

		// Search config in `$HOME/.config/frostfs-cli/` with name "config.yaml"
		viper.AddConfigPath(filepath.Join(home, ".config", "frostfs-cli"))
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		common.PrintVerbose(rootCmd, "Using config file: %s", viper.ConfigFileUsed())
	}
}
