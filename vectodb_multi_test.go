package vectodb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sizeLimit int = 1000
)

func TestVectodbMulti(t *testing.T) {
	var err error
	err = VectodbMultiClearWorkDir(workDir)
	require.NoError(t, err)

	vm, err := NewVectodbMulti(workDir, dim, sizeLimit)
	require.NoError(t, err)

	vm.StartBuilderLoop()

	const nb int = 100000
	xb := make([]float32, nb*dim)
	xids := make([]int64, nb)
	var xidBegin int64
	xidBegin, err = vm.AllocateIds()
	require.NoError(t, err)

	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			xb[i*dim+j] = rand.Float32()
		}
		normalizeInplace(dim, xb[i*dim:(i+1)*dim])
		xids[i] = xidBegin + int64(i)
	}

	err = vm.AddWithIds(xb, xids)
	require.NoError(t, err)

	var res [][]XidScore
	res, err = vm.Search(nb/10, 1, xb)
	require.NoError(t, err)
	fmt.Printf("res: %+v\n", res)

	time.Sleep(5 * time.Second)

	vm.StopBuilderLoop()
}
