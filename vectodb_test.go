package vectodb

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	dim    int = 2
	metric int = 1 //0 - IP, 1 - L2

	indexkey    string  = "Flat"
	queryParams string  = ""
	distThr     float32 = 0.8
	flatThr     int     = 0
	//indexkey    string = "IVF4096,PQ32"
	//queryParams string = "nprobe=256,ht=256"

	vdbWorkDir string = "/tmp/vectodb_test_go"
)

func TestVectodbNew(t *testing.T) {
	var err error
	VectodbClearWorkDir(vdbWorkDir)
	vdb, err := NewVectoDB(vdbWorkDir, dim, metric, indexkey, queryParams, distThr, flatThr)
	require.NoError(t, err)
	err = vdb.Destroy()
	require.NoError(t, err)
}

func l2distance(d int, v1, v2 []float32) (distance float32) {
	distance = 0
	for i := 0; i < d; i++ {
		distance += (v1[i] - v2[i]) * (v1[i] - v2[i])
	}
	return distance
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

func diff(d int, xb []float32, xids, I []int64, D []float32) {
	for i := 0; i < len(xids); i++ {
		i1 := int(xids[i])
		i2 := int(I[i])
		if i1 != i2 {
			v1 := xb[i1*d : (i1+1)*d]
			v2 := xb[i2*d : (i2+1)*d]
			dis := l2distance(dim, v1, v2)
			fmt.Printf("D[%d]=%v, dis(%d, %d)=%v, xb[%d]=%v, xb[%d]=%v\n", i1, D[i1], i1, i2, dis, i1, v1, i2, v2)
		}
	}

}

func TestVectodbUpdate(t *testing.T) {
	var err error
	VectodbClearWorkDir(vdbWorkDir)
	vdb, err := NewVectoDB(vdbWorkDir, dim, metric, indexkey, queryParams, distThr, flatThr)
	require.NoError(t, err)

	const nb int = 100
	//const nb int = 200 //TODO: why 200 failed?
	xb := make([]float32, nb*dim)
	xids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			xb[i*dim+j] = rand.Float32()
		}
		normalizeInplace(dim, xb[i*dim:(i+1)*dim])
	}

	for i := 0; i < nb; i++ {
		v := xb[i*dim : (i+1)*dim]
		dis := l2distance(dim, v, v)
		require.Equal(t, dis, float32(0))
	}

	err = vdb.AddWithIds(xb, xids)
	require.NoError(t, err)
	expXids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		expXids[i] = int64(i)
	}
	require.Equal(t, expXids, xids)

	total, err := vdb.GetTotal()
	require.NoError(t, err)
	require.Equal(t, nb, total)

	D := make([]float32, nb)
	I := make([]int64, nb)

	total, err = vdb.Search(xb, D, I)
	require.NoError(t, err)
	require.Equal(t, nb, total)
	fmt.Printf("D: %+v\n", D)
	fmt.Printf("I: %+v\n", I)
	diff(dim, xb, xids, I, D)
	require.Equal(t, xids, I)

	// update with the same vector
	err = vdb.UpdateWithIds(xb, xids)
	require.NoError(t, err)

	err = vdb.UpdateIndex()
	require.NoError(t, err)

	D2 := make([]float32, nb)
	I2 := make([]int64, nb)

	total2, err := vdb.Search(xb, D2, I2)
	require.NoError(t, err)
	require.Equal(t, nb, total2)
	fmt.Printf("D2: %+v\n", D2)
	fmt.Printf("I2: %+v\n", I2)
	diff(dim, xb, xids, I2, D2)
	require.Equal(t, xids, I2)

	err = vdb.Destroy()
	require.NoError(t, err)

	vdb2, err := NewVectoDB(vdbWorkDir, dim, metric, indexkey, queryParams, distThr, flatThr)
	require.NoError(t, err)
	total3, err := vdb2.Search(xb, D2, I2)
	require.NoError(t, err)
	require.Equal(t, nb, total3)
	fmt.Printf("D2: %+v\n", D2)
	fmt.Printf("I2: %+v\n", I2)
	diff(dim, xb, xids, I2, D2)
	require.Equal(t, xids, I2)

	err = vdb2.Destroy()
	require.NoError(t, err)
}
