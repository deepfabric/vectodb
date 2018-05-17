package vectodb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	disThr    float32 = 1e-5
	sizeLimit int     = 1000
)

func TestVectodbMulti(t *testing.T) {
	var err error
	err = VectodbMultiClearWorkDir(workDir)
	require.NoError(t, err)

	vm, err := NewVectodbMulti(workDir, dim, metric, indexkey, queryParams, sizeLimit)
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

	err = vm.AddWithIds(nb, xb, xids)
	require.NoError(t, err)

	var I []int64
	I, err = vm.Search(disThr, nb/10, xb)
	require.NoError(t, err)
	fmt.Printf("I: %+v\n", I)

	// modify every component of even line
	nb2 := nb / 2
	xb2 := make([]float32, nb2*dim)
	xids2 := make([]int64, nb2)
	for i := 0; i < nb2; i++ {
		for j := 0; j < dim; j++ {
			xb2[i*dim+j] = rand.Float32()
		}
		normalizeInplace(dim, xb2[i*dim:(i+1)*dim])
		xids2[i] = int64(2 * i)
	}

	err = vm.UpdateWithIds(nb2, xb2, xids2)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	vm.StopBuilderLoop()
}
