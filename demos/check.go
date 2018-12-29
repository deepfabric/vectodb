package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"

	"github.com/infinivision/vectodb"
	log "github.com/sirupsen/logrus"
)

var (
	path    = flag.String("data", "/tmp/checker", "VectorDB: data path")
	flatThr = flag.Int("flat", 1000, "VectorDB: flatThr")
	distThr = flag.Float64("dist", 0.9, "VectorDB: distThr")
	source  = flag.String("source", "/data/", "VectorDB: source file")

	siftDim int = 128
	numVecs int = 100
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
	flag.Parse()

	if err := vectodb.VectodbClearWorkDir(*path); err != nil {
		log.Fatalf("VectodbClearWorkDir failed, error:%+v", err)
	}
	db, err := vectodb.NewVectoDB(*path, siftDim, 0, "IVF4096,PQ32", "nprobe=256,ht=256", float32(*distThr), *flatThr)
	if err != nil {
		log.Fatalf("init vector db failed, error:%+v", err)
	}

	xids := make([]int64, 1, 1)
	for i := 0; i < numVecs; i++ {
		vec := genVec()
		xids[0] = rand.Int63()
		err := db.AddWithIds(vec, xids)
		if err != nil {
			log.Fatalf("add with error: %+v", err)
		}

		distances := make([]float32, 1, 1)
		targetXids := make([]int64, 1, 1)
		var ntotal int
		ntotal, err = db.Search(vec, distances, targetXids)
		if err != nil {
			log.Fatalf("query with error: %+v", err)
		}
		if ntotal != i+1 {
			log.Fatalf("ntotal is incorrect, have %v, want %v", ntotal, i+1)
		}

		delta := distances[0] - 1.0
		if targetXids[0] == -1 {
			log.Fatalf("not found")
		} else if targetXids[0] != xids[0] {
			log.Fatal("targetXids[0] is incorrect, have %v, want %v", targetXids[0], xids[0])
		} else if delta < -0.001 || delta > 0.001 {
			log.Fatalf("distances[0] is incorrect, have %v, want %v", distances[0], 1.0)
		}
		fmt.Print(".")
	}
	fmt.Println("pass")
}
