package main

import (
	"log"

	"github.com/infinivision/vectodb"
)

func main() {
	var err error
	var vdb *vectodb.VectoDB
	if vdb, err = vectodb.NewVectoDB("/tmp", 128, 1, "IVF4096,PQ32", "nprobe=256,ht=256"); err != nil {
		log.Fatalf("%+v", err)
	}
	if err = vdb.Destroy(); err != nil {
		log.Fatalf("%+v", err)
	}
}
