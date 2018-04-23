package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lboost_filesystem -lboost_system -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++
// #include "vectodb.h"
// #include <stdlib.h>
import "C"

import (
	"sync"
	"unsafe"
)

type VectoDB struct {
	rwlock sync.RWMutex
	vdb_c  unsafe.Pointer
}

func NewVectoDB(workDir string, dim int, metricType int, indexKey string, queryParams string) (vdb *VectoDB, err error) {
	workDir_c := C.CString(workDir)
	indexKey_c := C.CString(indexKey)
	queryParams_c := C.CString(queryParams)
	vdb_c := C.VectodbNew(workDir_c, C.long(dim), C.int(metricType), indexKey_c, queryParams_c)
	vdb = &VectoDB{
		vdb_c: vdb_c,
	}
	C.free(unsafe.Pointer(workDir_c))
	C.free(unsafe.Pointer(indexKey_c))
	C.free(unsafe.Pointer(queryParams_c))
	return
}

func (vdb *VectoDB) Destroy() (err error) {
	C.VectodbDelete(vdb.vdb_c)
	return
}

func (vdb *VectoDB) BuildIndex(cur_ntrain, cur_ntotal int) (index unsafe.Pointer, ntrain int, err error) {
	var ntrain_c C.long
	index = C.VectodbBuildIndex(vdb.vdb_c, C.long(cur_ntrain), C.long(cur_ntotal), &ntrain_c)
	ntrain = int(ntrain_c)
	return
}

/**
 * Writer methods. There could be multiple writers.
 */

func (vdb *VectoDB) ActivateIndex(index unsafe.Pointer, ntrain int) (err error) {
	vdb.rwlock.Lock()
	defer vdb.rwlock.Unlock()
	C.VectodbActivateIndex(vdb.vdb_c, index, C.long(ntrain))
	return
}

func (vdb *VectoDB) AddWithIds(nb int, xb []float32, xids []int64) (err error) {
	vdb.rwlock.Lock()
	defer vdb.rwlock.Unlock()
	C.VectodbAddWithIds(vdb.vdb_c, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

/**
 * Reader methods. There could be multiple readers.
 */
func (vdb *VectoDB) GetIndexState() (ntrain, ntotal, nflat int, err error) {
	vdb.rwlock.RLock()
	defer vdb.rwlock.RUnlock()
	var ntrain_c, ntotal_c, nflat_c C.long
	C.VectodbGetIndexState(vdb.vdb_c, &ntrain_c, &ntotal_c, &nflat_c)
	ntrain = int(ntrain_c)
	ntotal = int(ntotal_c)
	nflat = int(nflat_c)
	return
}

func (vdb *VectoDB) Search(nq int, xq []float32, distances []float32, xids []int64) (err error) {
	vdb.rwlock.RLock()
	C.VectodbSearch(vdb.vdb_c, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	vdb.rwlock.RUnlock()
	return
}

/**
 * Static methods.
 */

func VectodbClearWorkDir(workDir string) (err error) {
	workDir_c := C.CString(workDir)
	C.VectodbClearWorkDir(workDir_c)
	C.free(unsafe.Pointer(workDir_c))
	return
}
