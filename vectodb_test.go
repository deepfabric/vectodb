package vectodb

import (
	fmt "fmt"
	"testing"

	"github.com/RoaringBitmap/roaring"
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

func TestCkbitmap(t *testing.T) {
	var err error
	var buf []byte
	nums := []int{0, 1, small_set_size - 1, small_set_size, small_set_size + 1, 10000, 1000000}
	for _, num := range nums {
		fmt.Printf("TestCkbitmap, num=%d\n", num)
		rb := roaring.NewBitmap()
		for i := 0; i < num; i++ {
			rb.Add(uint32(i))
		}
		buf, err = ChBitmapSerialize(rb)
		require.NoError(t, err)

		var rb2 *roaring.Bitmap
		rb2, err = ChBitmapDeserialize(buf)
		require.NoError(t, err)
		rb2.Xor(rb)
		require.Equal(t, uint64(0), rb2.GetCardinality())
	}

}
