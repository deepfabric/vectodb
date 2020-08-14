package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	runPprof "runtime/pprof"
	"sync/atomic"
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
	workDir    string = "/data/sdc/demo_sift1M_vectodb_go"
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
			if res, err = vdb.Search(k, true, xq, uids); err != nil {
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

/**
 * @param vecs_per_user: how many pictures each user has
 * @param bm_card: bitmap cardinality in search. negative value means all.
 */
func demo_search_bitmap(d, nb int, xb []float32, vecs_per_user, nq, k int, top_vectors bool, bm_card int) {
	var err error
	var vdb *vectodb.VectoDB

	// TODO: Comment out following line, Reset() crashs!
	vectodb.VectodbClearWorkDir(workDir)
	if vdb, err = vectodb.NewVectoDB(workDir, d); err != nil {
		log.Fatalf("%+v", err)
	}
	vdb.Reset()

	xids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		xids[i] = int64(vectodb.GetXid(uint64(i/vecs_per_user), uint64(i)))
	}

	log.Infof("Calling vdb.AddWithIds %v", nb)
	if err = vdb.AddWithIds(xb, xids); err != nil {
		log.Fatalf("%+v", err)
	}

	xq := xb[0 : d*nq]
	rbs := make([]*roaring.Bitmap, nq)
	var uids [][]byte
	if bm_card >= 0 {
		uids = make([][]byte, nq)
		for i := 0; i < nq; i++ {
			uid := uint64(i / vecs_per_user)
			rbs[i] = roaring.NewBitmap()
			rbs[i].AddRange(uid, uid+uint64(bm_card))
			rbs[i].RunOptimize()
			if rbs[i].GetCardinality() != uint64(bm_card) {
				log.Fatalf("rbs[i].GetCardinality()", i, rbs[i].GetCardinality())
			}
			if uids[i], err = vectodb.ChBitmapSerialize(rbs[i]); err != nil {
				log.Fatalf("%+v", err)
			}
		}
	}

	log.Infof("Searching index")
	var res [][]vectodb.XidScore
	if res, err = vdb.Search(k, false, xq, uids); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Debugf("search result: %+v", res)
	if bm_card >= 0 {
		i := 0
		log.Infof("checking result of query %d", i)
		msg := fmt.Sprintf("len(res[%d])=%d", i, len(res[i]))
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

func demo_search_shards(d, nb int, xb []float32, vecs_per_user, nq, k int, top_vectors bool, bm_card, shards, threads int) {
	var err error
	vdbs := make([]*vectodb.VectoDB, shards)
	for i := 0; i < shards; i++ {
		dir := fmt.Sprintf("%s.s%d", workDir, i)
		log.Infof("populating data into shard %s", dir)
		vectodb.VectodbClearWorkDir(dir)
		var vdb *vectodb.VectoDB
		if vdb, err = vectodb.NewVectoDB(dir, d); err != nil {
			log.Fatalf("%+v", err)
		}
		xids := make([]int64, nb)
		for j := 0; i < nb; i++ {
			xids[j] = int64(vectodb.GetXid(uint64((i*nb+j)/vecs_per_user), uint64(i*nb+j)))
		}
		if err = vdb.AddWithIds(xb, xids); err != nil {
			log.Fatalf("%+v", err)
		}
		vdbs[i] = vdb
	}

	xq := xb[0 : d*nq]
	rbs := make([]*roaring.Bitmap, nq)
	var uids [][]byte
	if bm_card >= 0 {
		uids = make([][]byte, nq)
		for i := 0; i < nq; i++ {
			uid := uint64(i / vecs_per_user)
			rbs[i] = roaring.NewBitmap()
			rbs[i].AddRange(uid, uid+uint64(bm_card))
			rbs[i].RunOptimize()
			if rbs[i].GetCardinality() != uint64(bm_card) {
				log.Fatalf("rbs[i].GetCardinality()", i, rbs[i].GetCardinality())
			}
			if uids[i], err = vectodb.ChBitmapSerialize(rbs[i]); err != nil {
				log.Fatalf("%+v", err)
			}
		}
	}

	var cnt_queries int64
	stop := false
	for i := 0; i < shards; i++ {
		go func(shard int) {
			for stop != false {
				if _, err = vdbs[i].Search(k, false, xq, uids); err != nil {
					log.Fatalf("%+v", err)
				}
				atomic.AddInt64(&cnt_queries, int64(nq))
			}
		}(i)
	}

	cnt1 := int64(0)
	t1 := time.Now()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(60) * time.Second)
		t2 := time.Now()
		duration := t2.Sub(t1)
		cnt2 := atomic.LoadInt64(&cnt_queries)
		log.Info("%v queries in %v seconds, %v qps", float64(cnt2-cnt1)/float64(shards), duration.Seconds(), float64(cnt2-cnt1)/float64(float64(shards)*duration.Seconds()))
		cnt1 = cnt2
		t1 = t2
	}

	for i := 0; i < shards; i++ {
		if err = vdbs[i].Destroy(); err != nil {
			log.Fatalf("%+v", err)
		}
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
	var d int
	var nb int
	if xb, d, nb, err = fvecs_read(siftBase); err != nil {
		log.Fatalf("%+v", err)
	}
	if d != siftDim {
		log.Fatalf("%s d %d, expects 128", siftBase, d)
	}

	nb2 := 3125000
	d2 := 256
	if nb2*d2 > nb*d {
		for len(xb) < nb2*d2 {
			xb = append(xb, xb[:nb*d]...)
		}
		xb = xb[:nb2*d2]
	}
	nb = nb2
	d = d2
	log.Infof("d = %d, nb = %d", d, nb)

	for i := 0; i < nb; i++ {
		vectodb.NormalizeVec(d, xb[i*d:(i+1)*d])
	}

	log.Info("demo_search_bitmap(d, nb, xb, 1, 1000, 400, true, -1)")
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, true, -1)

	log.Info("demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, -1)")
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, -1)

	log.Info("demo_search_bitmap(d, nb, xb, 100, 1000, 400, false, -1)")
	demo_search_bitmap(d, nb, xb, 100, 1000, 400, false, -1)

	log.Info("demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, 10)")
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, 10)

	log.Info("demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, 100000000)")
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, 100000000)

	log.Info("demo_search_bitmap(d, nb, xb, 100, 1000, 400, false, 100000000)")
	demo_search_bitmap(d, nb, xb, 100, 1000, 400, false, 100000000)

	//gpu07: Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz, 48 vcpu
	log.Info("demo_search_shards(d, nb, xb, 1, 1, 400, false, -1, shards, threads)")
	shards := 32
	threads := 1
	demo_search_shards(d, nb, xb, 1, 1, 400, false, -1, shards, threads)

}
