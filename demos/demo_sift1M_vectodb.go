package main

import (
	"log"
	"os"
	"syscall"
	"unsafe"

	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
)

//FileMmap mmaps the given file.
//https://medium.com/@arpith/adventures-with-mmap-463b33405223
func FileMmap(f *os.File) (data []byte, err error) {
	info, err1 := f.Stat()
	if err1 != nil {
		err = errors.Wrap(err1, "")
		return
	}
	prots := []int{syscall.PROT_WRITE | syscall.PROT_READ, syscall.PROT_READ}
	for _, prot := range prots {
		data, err = syscall.Mmap(int(f.Fd()), 0, int(info.Size()), prot, syscall.MAP_SHARED)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//FileMunmap unmaps the given file.
func FileMunmap(data []byte) (err error) {
	err = syscall.Munmap(data)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func fvecs_read(fname string) (x []float32, d, n int, err error) {
	var f *os.File
	var data []byte
	if f, err = os.OpenFile(fname, os.O_RDWR, 0600); err != nil {
		return
	}
	if data, err = FileMmap(f); err != nil {
		return
	}
	sz := len(data)
	d = int(*(*int32)(unsafe.Pointer(&data[0])))
	if sz%((d+1)*4) != 0 {
		err = errors.Errorf("weird file size")
		return
	}
	n = sz / ((d + 1) * 4)
	x = make([]float32, n*d)
	for i := 0; i < n; i++ {
		start := i*(d+1)*4 + 4
		for j := 0; j < d; j++ {
			x[i*d+j] = *(*float32)(unsafe.Pointer(&data[start+j*4]))
		}
	}

	if err = FileMunmap(data); err != nil {
		return
	}
	err = f.Close()
	return
}

func ivecs_read(fname string) (x []int32, d, n int, err error) {
	var x2 []float32
	if x2, d, n, err = fvecs_read(fname); err != nil {
		return
	}
	x = make([]int32, n*d)
	for i := 0; i < n*d; i++ {
		x[i] = *(*int32)(unsafe.Pointer(&x2[i]))
	}
	return
}

func main() {
	var err error
	var vdb *vectodb.VectoDB

	workDir := "/tmp"
	siftDim := 128
	if err = vectodb.VectodbClearWorkDir(workDir); err != nil {
		log.Fatalf("%+v", err)
	}
	if vdb, err = vectodb.NewVectoDB(workDir, siftDim, 1, "IVF4096,PQ32", "nprobe=256,ht=256"); err != nil {
		log.Fatalf("%+v", err)
	}

	log.Printf("Loading database")
	var xb []float32
	var dim int
	var nb int
	if xb, dim, nb, err = fvecs_read("sift1M/sift_base.fvecs"); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim != siftDim {
		log.Fatalf("sift_base.fvecs dim %d, expects 128", dim)
	}

	xids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		xids[i] = int64(i)
	}

	if err = vdb.AddWithIds(nb, xb, xids); err != nil {
		log.Fatalf("%+v", err)
	}

	var index unsafe.Pointer
	var ntrain int
	if index, ntrain, err = vdb.BuildIndex(); err != nil {
		log.Fatalf("%+v", err)
	}
	if err = vdb.ActivateIndex(index, ntrain); err != nil {
		log.Fatalf("%+v", err)
	}

	log.Printf("Searching index")
	var xq []float32
	var dim2 int
	var nq int
	if xq, dim2, nq, err = fvecs_read("sift1M/sift_query.fvecs"); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim2 != siftDim {
		log.Fatalf("sift_query.fvecs dim %d, expects %d", dim2, siftDim)
	}
	D := make([]float32, nq)
	I := make([]int64, nq)
	if err = vdb.Search(nq, xq, D, I); err != nil {
		log.Fatalf("%+v", err)
	}

	log.Printf("Loading ground truth for %d queries", nq)
	var gt []int32
	var k int
	var nq2 int
	if gt, k, nq2, err = ivecs_read("sift1M/sift_groundtruth.ivecs"); err != nil {
		log.Fatalf("%+v", err)
	}
	if nq2 != nq {
		log.Fatalf("sift1M/sift_groundtruth.ivecs nq %d, expects %d", nq2, nq)
	}

	log.Printf("Compute recalls")
	// evaluate result by hand.
	var n_1 int
	for i := 0; i < nq; i++ {
		gt_nn := int64(gt[i*k])
		if I[i] == gt_nn {
			n_1++
		}
	}
	log.Printf("R@1 = %v", float32(n_1)/float32(nq))

	if err = vdb.Destroy(); err != nil {
		log.Fatalf("%+v", err)
	}
}
