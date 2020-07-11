package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	runPprof "runtime/pprof"
	"syscall"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	siftDim    int    = 128
	siftBase   string = "sift1M/sift_base.fvecs"
	siftQuery  string = "sift1M/sift_query.fvecs"
	siftGround string = "sift1M/sift_groundtruth.ivecs"
	workDir    string = "/tmp/demo_sift1M_vectodb_go"
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
		vectodb.NormalizeVec(d, x[i*d:(i+1)*d])
	}

	if err = FileMunmap(data); err != nil {
		return
	}
	err = f.Close()
	return
}

func searcherLoop(ctx context.Context, vdb *vectodb.VectoDB) {
	var err error
	log.Infof("Searching index")
	var xq []float32
	var dim2 int
	var nq int
	var k int
	if xq, dim2, nq, err = fvecs_read(siftQuery); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim2 != siftDim {
		log.Fatalf("%s dim %d, expects %d", siftQuery, dim2, siftDim)
	}

	nq = 500
	k = 10
	var res [][]vectodb.XidScore
	uids := make([][]byte, nq)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Infof("search iteration begin")
			if res, err = vdb.Search(k, xq, uids); err != nil {
				log.Fatalf("%+v", err)
			}
			log.Infof("search iteration done, res=%v", res)
		}
	}
}

func benchmarkAdd() {
	var err error
	var vdb *vectodb.VectoDB

	if err = vectodb.VectodbClearWorkDir(workDir); err != nil {
		log.Fatalf("%+v", err)
	}
	if vdb, err = vectodb.NewVectoDB(workDir, siftDim); err != nil {
		log.Fatalf("%+v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go searcherLoop(ctx, vdb)

	log.Infof("Loading database")
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
		vdb.AddWithIds(xb[i*batchSize*siftDim:(i+1)*batchSize*siftDim], xids[i*batchSize:(i+1)*batchSize])
	}
	log.Infof("added %d vectors in %v\n", nb, time.Since(begin))
	cancel()
	time.Sleep(1 * time.Second)
	return
}

func demo_search(d, nb int, xb []float32) {
	var err error
	var vdb *vectodb.VectoDB

	if vdb, err = vectodb.NewVectoDB(workDir, d); err != nil {
		log.Fatalf("%+v", err)
	}
	vdb.Reset()

	xids := make([]int64, nb)
	id_shift := int64(1000)
	for i := 0; i < nb; i++ {
		xids[i] = id_shift + int64(i)
	}

	log.Infof("Inserting vectors")
	if err = vdb.AddWithIds(xb, xids); err != nil {
		log.Fatalf("%+v", err)
	}

	log.Infof("Searching index")
	nq := 1000
	k := 400
	xq := xb[0 : d*nq]
	var res [][]vectodb.XidScore
	if res, err = vdb.Search(k, xq, nil); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Debugf("search result: %+v", res)

	if err = vdb.Destroy(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func demo_search_roaring(d, nb int, xb []float32) {
	var err error
	var vdb *vectodb.VectoDB

	if vdb, err = vectodb.NewVectoDB(workDir, d); err != nil {
		log.Fatalf("%+v", err)
	}
	vdb.Reset()

	xids := make([]int64, nb)
	vecs_per_user := uint64(100)
	for i := 0; i < nb; i++ {
		xids[i] = int64(vectodb.GetXid(uint64(i)/vecs_per_user, uint64(i)))
	}

	log.Infof("Inserting vectors")
	if err = vdb.AddWithIds(xb, xids); err != nil {
		log.Fatalf("%+v", err)
	}

	nq := 1000
	k := 400
	xq := xb[0 : d*nq]
	rbs := make([]*roaring.Bitmap, nq)
	uids := make([][]byte, nq)
	bitmap_cardinality := uint64(3)
	for i := 0; i < nq; i++ {
		uid := uint64(i) / vecs_per_user
		rbs[i] = roaring.NewBitmap()
		rbs[i].AddRange(uint64(uid), uint64(uid+bitmap_cardinality))
		if uids[i] = vectodb.ChBitmapToBytes(rbs[i]); err != nil {
			log.Fatalf("%+v", err)
		}
	}

	log.Infof("Searching index")
	var res [][]vectodb.XidScore
	if res, err = vdb.Search(k, xq, uids); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Debugf("search result: %+v", res)
	log.Infof("checking result")
	for i := 0; i < nq; i++ {
		msg := fmt.Sprintf("i %d", i)
		for j := 0; j < len(res[i]); j++ {
			xid := res[i][j].Xid
			uid := vectodb.GetUid(uint64(xid))
			pid := vectodb.GetPid(uint64(xid))
			msg += fmt.Sprintf(", %d-%d %f", uid, pid, res[i][j].Score)
			if !rbs[i].Contains(uint32(uid)) {
				log.Fatalf("Bitmap filter bug, i %d, j %d, xid %d, uid-pid %d-%d", i, j, xid, uid, pid)
			}
		}
		log.Info(msg)
	}

	if err = vdb.Destroy(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func main() {
	var bench bool
	var verbose bool
	flag.BoolVar(&bench, "b", false, "benchmark (*VectoDB)AddWithIds")
	flag.BoolVar(&verbose, "v", false, "verbose output")
	flag.Parse()
	if verbose {
		log.SetLevel(log.DebugLevel)
	}
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
	})

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
					log.Infof("got signal=<%d>.", sig)
					log.Println(buf.String())
					continue
				}
			}
		}()
		benchmarkAdd()
		return
	}

	log.Infof("Loading database")
	var err error
	var xb []float32
	var dim int
	var nb int
	if xb, dim, nb, err = fvecs_read(siftBase); err != nil {
		log.Fatalf("%+v", err)
	}
	if dim != siftDim {
		log.Fatalf("%s dim %d, expects 128", siftBase, dim)
	}
	for i := 0; i < nb; i++ {
		vectodb.NormalizeVec(dim, xb[i*dim:(i+1)*dim])
	}

	demo_search(dim, nb, xb)
	demo_search_roaring(dim, nb, xb)
}
