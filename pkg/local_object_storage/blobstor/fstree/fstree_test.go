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
	require.Equal(t, addr, actual)
}

func Benchmark_addressFromString(b *testing.B) {
	addr := oidtest.Address()
	s := stringifyAddress(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := addressFromString(s)
		if err != nil {
			b.Fatalf("benchmark error: %v", err)
		}
	}
}
