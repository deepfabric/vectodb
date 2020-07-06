package vectodb

import (
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
