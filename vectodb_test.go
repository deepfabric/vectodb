package vectodb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	dim     int    = 128
	workDir string = "/tmp/vectodb_test_go"
)

func TestVectodbNew(t *testing.T) {
	var err error
	VectodbClearWorkDir(workDir)
	vdb, err := NewVectoDB(workDir, dim)
	require.NoError(t, err)
	err = vdb.Destroy()
	require.NoError(t, err)
}

func normalizeInplace(d int, v []float32) {
	var norm float32
	for i := 0; i < d; i++ {
		norm += v[i] * v[i]
	}
	norm = float32(math.Sqrt(float64(norm)))
	for i := 0; i < d; i++ {
		v[i] /= norm
	}
}
