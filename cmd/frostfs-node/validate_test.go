package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config"
	configtest "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	const exampleConfigPrefix = "../../config/"
	t.Run("examples", func(t *testing.T) {
		p := filepath.Join(exampleConfigPrefix, "example/node")
		configtest.ForEachFileType(p, func(c *config.Config) {
			var err error
			require.NotPanics(t, func() {
				err = validateConfig(c)
			})
			require.NoError(t, err)
		})
	})

	t.Run("mainnet", func(t *testing.T) {
		os.Clearenv() // ENVs have priority over config files, so we do this in tests
		p := filepath.Join(exampleConfigPrefix, "mainnet/config.yml")
		c := config.New(config.Prm{}, config.WithConfigFile(p))
		require.NoError(t, validateConfig(c))
	})
	t.Run("testnet", func(t *testing.T) {
		os.Clearenv() // ENVs have priority over config files, so we do this in tests
		p := filepath.Join(exampleConfigPrefix, "testnet/config.yml")
		c := config.New(config.Prm{}, config.WithConfigFile(p))
		require.NoError(t, validateConfig(c))
	})
}
