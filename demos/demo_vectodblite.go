package main

import (
	"bytes"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	runPprof "runtime/pprof"
)

const (
	redisAddr string  = "127.0.0.1:6379"
	siftDim   int     = 128
	distThr   float32 = 0.6

	sizeLimit int = 100
	numVecs   int = sizeLimit + 5
)

func genVec() (vec []float32) {
	vec = make([]float32, siftDim)
	var prod float64
	for i := 0; i < siftDim; i++ {
		vec[i] = rand.Float32()
		prod += float64(vec[i]) * float64(vec[i])
	}
	prod = math.Sqrt(prod)
	for i := 0; i < siftDim; i++ {
		vec[i] = float32(float64(vec[i]) / prod)
	}
	return
}

func main() {
	go func() {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
			syscall.SIGUSR1)
		for {
			sig := <-sc
			switch sig {
			case syscall.SIGUSR1:
				buf := bytes.NewBuffer([]byte{})
				_ = runPprof.Lookup("goroutine").WriteTo(buf, 1)
				log.Infof("got signal=<%d>.", sig)
				log.Infof(buf.String())
				continue
			default:
				retVal := 0
				if sig != syscall.SIGTERM {
					retVal = 1
				}
				log.Infof("exit: signal=<%d>.", sig)
				log.Infof("exit: bye :-).")
				os.Exit(retVal)
			}
		}
	}()

	var err error
	var vdbl *vectodb.VectoDBLite
	if vdbl, err = vectodb.NewVectoDBLite(redisAddr, 0, siftDim, distThr, sizeLimit); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatalf("%+v", err)
	}

	log.Printf("VectoDBLite size %v", vdbl.Size())

	xids := make([]uint64, numVecs)
	vecs := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vecs[i] = genVec()
		if xids[i], err = vdbl.Add(vecs[i]); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatalf("%+v", err)
		}
		log.Infof("added xid %016x, vec %v", xids[i], vecs[i])
	}

	log.Printf("Searching index")
	for i := 0; i < numVecs; i++ {
		var xid uint64
		var distance float32
		if xid, distance, err = vdbl.Search(vecs[i]); err != nil {
			err = errors.Wrapf(err, "")
			log.Fatalf("i=%d, %+v", i, err)
		}
		if xid != xids[i] {
			log.Infof("i=%d, xid mismatch, want %016x, have %016x", i, xids[i], xid)
		}
		if distance < distThr {
			log.Fatalf("i=%d, distance incorrect, want >=%v, have %v", i, distThr, distance)
		}
	}
	time.Sleep(15)
}
