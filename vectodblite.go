package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lfaiss -lopenblas -lgomp -lstdc++ -ljemalloc
// #include "index_flat_wrapper.h"
// #include <stdlib.h>
import "C"

import (
	"unsafe"

	log "github.com/sirupsen/logrus"
)

// VectoDBLite is tiny stateless non-updatable non-removable vector database. Only supports metric type 0 - METRIC_INNER_PRODUCT.
type VectoDBLite struct {
	redisURL      string
	dbID          int
	dim           int
	distThreshold float32
	flatC         unsafe.Pointer
}

func NewVectoDBLite(redisURL string, dbID int, dimIn int, distThreshold float32) (vdbl *VectoDBLite, err error) {
	log.Infof("creating VectoDBLite %v %v", redisURL, dbID)
	flatC := C.IndexFlatNew(C.long(dimIn), C.float(distThreshold))
	vdbl = &VectoDBLite{
		redisURL:      redisURL,
		dbID:          dbID,
		dim:           dimIn,
		distThreshold: distThreshold,
		flatC:         flatC,
	}
	return
}

func (vdbl *VectoDBLite) Destroy() (err error) {
	log.Infof("destroying VectoDBLite %v %v", vdbl.redisURL, vdbl.dbID)
	C.IndexFlatDelete(vdbl.flatC)
	vdbl.flatC = nil
	return
}

func (vdbl *VectoDBLite) AddWithIds(xb []float32, xids []int64) (err error) {
	nb := len(xids)
	if len(xb) != nb*vdbl.dim {
		log.Fatalf("invalid length of xb, want %v, have %v", nb*vdbl.dim, len(xb))
	}
	C.IndexFlatAddWithIds(vdbl.flatC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdbl *VectoDBLite) Search(xq []float32, distances []float32, xids []int64) (err error) {
	nq := len(xids)
	if len(xq) != nq*vdbl.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", nq*vdbl.dim, len(xq))
	}
	if len(distances) != nq {
		log.Fatalf("invalid length of distances, want %v, have %v", nq, len(distances))
	}
	C.IndexFlatSearch(vdbl.flatC, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	return
}
