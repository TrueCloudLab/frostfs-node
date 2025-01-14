package client

import (
	"math/big"
	"testing"

	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/stretchr/testify/require"
)

func TestToStackParameter(t *testing.T) {
	items := []struct {
		value   any
		expType sc.ParamType
		expVal  any
	}{
		{
			value:   []byte{1, 2, 3},
			expType: sc.ByteArrayType,
		},
		{
			value:   int64(100),
			expType: sc.IntegerType,
			expVal:  big.NewInt(100),
		},
		{
			value:   uint64(100),
			expType: sc.IntegerType,
			expVal:  big.NewInt(100),
		},
		{
			value:   "hello world",
			expType: sc.StringType,
		},
		{
			value:   false,
			expType: sc.BoolType,
		},
		{
			value:   true,
			expType: sc.BoolType,
		},
	}

	for _, item := range items {
		t.Run(item.expType.String()+" to stack parameter", func(t *testing.T) {
			res, err := toStackParameter(item.value)
			require.NoError(t, err)
			require.Equal(t, item.expType, res.Type)
			if item.expVal != nil {
				require.Equal(t, item.expVal, res.Value)
			} else {
				require.Equal(t, item.value, res.Value)
			}
		})
	}
}
