package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	runPprof "runtime/pprof"
	"syscall"
	"time"
	"unsafe"

	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
)

const (
	siftDim    int = 128
	siftMetric int = 1 //0 - IP, 1 - L2

	siftBase   string = "sift1M/sift_base.fvecs"
	siftQuery  string = "sift1M/sift_query.fvecs"
	siftGround string = "sift1M/sift_groundtruth.ivecs"

	siftIndexKey    string = "IVF4096,PQ32"
	siftQueryParams string = "nprobe=256,ht=256"
	//siftIndexKey    string = "IVF16384_HNSW32,Flat"
	//siftQueryParams string = "nprobe=384"

	workDir       string = "/tmp/demo_sift1M_vectodb_go"
	flatThreshold int    = 1000
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

func builderLoop(ctx context.Context, vdb *vectodb.VectoDB) {
	ticker := time.Tick(5 * time.Second)
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			log.Printf("build iteration begin")
			if err = vdb.UpdateIndex(); err != nil {
				log.Fatalf("%+v", err)
			}
			log.Printf("build iteration done")
		}
	}
}

func searcherLoop(ctx context.Context, vdb *vectodb.VectoDB) {
	var err error
	log.Printf("Searching index")
	var xq []float32
	var dim2 int
	var nq int
	if xq, dim2, nq, err = fvecs_read(siftQuery); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim2 != siftDim {
		log.Fatalf("%s dim %d, expects %d", siftQuery, dim2, siftDim)
	}
	nq = 500
	D := make([]float32, nq)
	I := make([]int64, nq)
	var nflat, ntotal int
	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Printf("search iteration begin")
			if nflat, err = vdb.GetFlatSize(); err != nil {
				log.Fatalf("%+v", err)
			}
			log.Printf("nflat %d", nflat)
			if ntotal, err = vdb.Search(nq, xq, D, I); err != nil {
				log.Fatalf("%+v", err)
			}
			log.Printf("search iteration done, ntotal=%d", ntotal)
		}
	}
}

func benchmarkAdd() {
	var err error
	var vdb *vectodb.VectoDB

	if err = vectodb.VectodbClearWorkDir(workDir); err != nil {
		log.Fatalf("%+v", err)
	}
	if vdb, err = vectodb.NewVectoDB(workDir, siftDim, siftMetric, siftIndexKey, siftQueryParams, flatThreshold); err != nil {
		log.Fatalf("%+v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go builderLoop(ctx, vdb)
	go searcherLoop(ctx, vdb)

	log.Printf("Loading database")
	var xb []float32
	var dim int
	var nb int
	if xb, dim, nb, err = fvecs_read(siftBase); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim != siftDim {
		log.Fatalf("%s dim %d, expects 128", siftBase, dim)
	}

	xids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		xids[i] = int64(i)
	}

	begin := time.Now()
	batchSize := 2
	for i := 0; i < nb/batchSize; i++ {
		vdb.AddWithIds(batchSize, xb[i*batchSize:(i+1)*batchSize], xids[i*batchSize:(i+1)*batchSize])
	}
	log.Printf("added %d vectors in %v\n", nb, time.Since(begin))
	cancel()
	time.Sleep(1 * time.Second)
	return
}

func main() {
	var bench bool
	flag.BoolVar(&bench, "b", false, "benchmark (*VectoDB)AddWithIds")
	flag.Parse()
	if bench {
		go func() {
			sc := make(chan os.Signal, 1)
			signal.Notify(sc, syscall.SIGUSR1)
			for {
				sig := <-sc
				switch sig {
				case syscall.SIGUSR1:
					buf := bytes.NewBuffer([]byte{})
					_ = runPprof.Lookup("goroutine").WriteTo(buf, 1)
					log.Printf("got signal=<%d>.", sig)
					log.Println(buf.String())
					continue
				}
			}
		}()
		benchmarkAdd()
		return
	}

	var err error
	var vdb *vectodb.VectoDB

	//if err = vectodb.VectodbClearWorkDir(workDir); err != nil {
	//	log.Fatalf("%+v", err)
	//}
	if vdb, err = vectodb.NewVectoDB(workDir, siftDim, siftMetric, siftIndexKey, siftQueryParams, flatThreshold); err != nil {
		log.Fatalf("%+v", err)
	}

	log.Printf("Loading database")
	var xb []float32
	var dim int
	var nb int
	if xb, dim, nb, err = fvecs_read(siftBase); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim != siftDim {
		log.Fatalf("%s dim %d, expects 128", siftBase, dim)
	}

	xids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		xids[i] = int64(i)
	}

	if err = vdb.AddWithIds(nb, xb, xids); err != nil {
		log.Fatalf("%+v", err)
	}

	if err = vdb.UpdateIndex(); err != nil {
		log.Fatalf("%+v", err)
	}

	log.Printf("Searching index")
	var xq []float32
	var dim2 int
	var nq int
	if xq, dim2, nq, err = fvecs_read(siftQuery); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim2 != siftDim {
		log.Fatalf("%s dim %d, expects %d", siftQuery, dim2, siftDim)
	}
	D := make([]float32, nq)
	I := make([]int64, nq)
	var ntotal int
	if ntotal, err = vdb.Search(nq, xq, D, I); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Printf("Search done on %d vectors", ntotal)

	log.Printf("Loading ground truth for %d queries", nq)
	var gt []int32
	var k int
	var nq2 int
	if gt, k, nq2, err = ivecs_read(siftGround); err != nil {
		log.Fatalf("%+v", err)
	}
	if nq2 != nq {
		log.Fatalf("%s nq %d, expects %d", siftGround, nq2, nq)
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
