package fstree

import (
	"testing"

	oidtest "github.com/TrueCloudLab/frostfs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestAddressToString(t *testing.T) {
	addr := oidtest.Address()
	s := stringifyAddress(addr)
	actual, err := addressFromString(s)
	require.NoError(t, err)
	require.Equal(t, addr, *actual)
}
