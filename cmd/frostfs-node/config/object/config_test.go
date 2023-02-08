package objectconfig_test

import (
	"testing"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config"
	objectconfig "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/object"
	configtest "github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestObjectSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Equal(t, objectconfig.PutPoolSizeDefault, objectconfig.Put(empty).PoolSizeRemote())
		require.EqualValues(t, objectconfig.DefaultTombstoneLifetime, objectconfig.TombstoneLifetime(empty))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 100, objectconfig.Put(c).PoolSizeRemote())
		require.EqualValues(t, 10, objectconfig.TombstoneLifetime(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
