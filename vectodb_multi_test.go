package vectodb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sizeLimit   int    = 1000
	vdbmWorkDir string = "/tmp/vectodb_multi_test_go"
)

func TestVectodbMulti(t *testing.T) {
	var err error
	err = VectodbMultiClearWorkDir(vdbmWorkDir)
	require.NoError(t, err)

	vm, err := NewVectodbMulti(vdbmWorkDir, dim, metric, indexkey, queryParams, distThr, sizeLimit)
	require.NoError(t, err)

	vm.StartBuilderLoop()

	const nb int = 100000
	xb := make([]float32, nb*dim)
	xids := make([]int64, nb)

	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			xb[i*dim+j] = rand.Float32()
		}
		normalizeInplace(dim, xb[i*dim:(i+1)*dim])
	}

	err = vm.AddWithIds(xb, xids)
	require.NoError(t, err)
	expXids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		expXids[i] = int64(i)
	}
	require.Equal(t, expXids, xids)

	var I []int64
	I, err = vm.Search(xb[:nb/100])
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

	err = vm.UpdateWithIds(xb2, xids2)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	vm.StopBuilderLoop()
}
