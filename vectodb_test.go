package vectodb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectodbNew(t *testing.T) {
	var err error
	vdb, err := NewVectoDB("/tmp", 128, 1, "IVF4096,PQ32", "nprobe=256,ht=256")
	require.NoError(t, err)
	err = vdb.Destroy()
	require.NoError(t, err)
}
