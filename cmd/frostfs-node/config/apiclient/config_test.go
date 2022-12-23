package apiclientconfig_test

import (
	"testing"
	"time"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config"
	apiclientconfig "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/apiclient"
	configtest "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestApiclientSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Equal(t, apiclientconfig.DialTimeoutDefault, apiclientconfig.DialTimeout(empty))
		require.Equal(t, apiclientconfig.StreamTimeoutDefault, apiclientconfig.StreamTimeout(empty))
		require.False(t, apiclientconfig.AllowExternal(empty))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 15*time.Second, apiclientconfig.DialTimeout(c))
		require.Equal(t, 20*time.Second, apiclientconfig.StreamTimeout(c))
		require.True(t, apiclientconfig.AllowExternal(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
