package internal_test

import (
	"testing"

	"github.com/TrueCloudLab/frostfs-node/cmd/frostfs-node/config/internal"
	"github.com/stretchr/testify/require"
)

func TestEnv(t *testing.T) {
	require.Equal(t,
		"FROSTFS_SECTION_PARAMETER",
		internal.Env("section", "parameter"),
	)
}
