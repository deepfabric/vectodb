package main

import (
	"flag"
	"os"
	"syscall"
	"unsafe"

	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	path    = flag.String("data", "/tmp/checker", "VectorDB: data path")
	flatThr = flag.Int("flat", 1000, "VectorDB: flatThr")
	distThr = flag.Float64("dist", 0.999, "VectorDB: distThr")
	source  = flag.String("source", "/data/", "VectorDB: source file")
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

func dotProduct(vec []float32) (prod float64) {
	for i := 0; i < len(vec); i++ {
		prod += float64(vec[i]) * float64(vec[i])
	}
	return prod
}

func main() {
	flag.Parse()

	data, d, n, err := fvecs_read(*source)
	if err != nil {
		log.Fatalf("fvecs_read failed with error:%+v", err)
	}

	if err := vectodb.VectodbClearWorkDir(*path); err != nil {
		log.Fatalf("VectodbClearWorkDir failed, error:%+v", err)
	}
	db, err := vectodb.NewVectoDB(*path, d, 0, "IVF4096,PQ32", "nprobe=256,ht=256", float32(*distThr), *flatThr)
	if err != nil {
		log.Fatalf("init vector db failed, error:%+v", err)
	}

	id := int64(0)
	xid := make([]int64, 1, 1)
	value := make([]float32, d, d)
	for i := 0; i < n; i++ {
		for j := 0; j < d; j++ {
			value[j] = data[d*i+j]
		}
		id++
		xid[0] = id
		err := db.AddWithIds(value, xid)
		if err != nil {
			log.Fatalf("add with error: %+v", err)
		}
		prod := dotProduct(value)
		if prod < 0.99 || prod > 1.01 {
			log.Fatalf("vector dot product is incorrect, have %v, want 1.0", prod)
		}
		log.Infof("insert: %d, prod: %v", id, prod)

		distances := make([]float32, 1, 1)
		targetXids := make([]int64, 1, 1)
		var ntotal int
		ntotal, err = db.Search(value, distances, targetXids)
		if err != nil {
			log.Fatalf("query with error: %+v", err)
		}
		if ntotal != i+1 {
			log.Fatalf("ntotal is incorrect, have %v, want %v", ntotal, i+1)
		}

		delta := float64(distances[0]) - prod
		if targetXids[0] == -1 {
			log.Fatalf("not found")
		} else if targetXids[0] != xid[0] {
			log.Fatal("targetXids[0] is incorrect, have %v, want %v", targetXids[0], xid[0])
		} else if delta < -0.001 || delta > 0.001 {
			log.Fatalf("distances[0] is incorrect, have %v, want %v", distances[0], prod)
		}
	}
}
