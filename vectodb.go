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

func NewVectoDB(workDir string, dim int64, metricType int, indexKey string, queryParams string) (vdb *VectoDB, err error) {
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

func (vdb *VectoDB) ActivateIndex(index unsafe.Pointer, ntrain int64) {
	vdb.rwlock.Lock()
	C.VectodbActivateIndex(vdb.vdb_c, index, C.long(ntrain))
	vdb.rwlock.Unlock()
}

func (vdb *VectoDB) AddWithIds(nb int64, xb []float32, xids []int64) {
	vdb.rwlock.Lock()
	C.VectodbAddWithIds(vdb.vdb_c, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	vdb.rwlock.Unlock()
}

/**
 * Reader methods. There could be multiple readers.
 */
func (vdb *VectoDB) TryBuildIndex(exhaust_threshold int64) (index unsafe.Pointer, ntrain int64) {
	vdb.rwlock.RLock()
	var ntrain_c C.long
	index = C.VectodbTryBuildIndex(vdb.vdb_c, C.long(exhaust_threshold), &ntrain_c)
	ntrain = int64(ntrain_c)
	vdb.rwlock.RUnlock()
	return
}

func (vdb *VectoDB) BuildIndex() (index unsafe.Pointer, ntrain int64) {
	vdb.rwlock.RLock()
	var ntrain_c C.long
	index = C.VectodbBuildIndex(vdb.vdb_c, &ntrain_c)
	ntrain = int64(ntrain_c)
	vdb.rwlock.RUnlock()
	return
}

func (vdb *VectoDB) Search(nq int64, xq []float32, distances []float32, xids []int64) {
	vdb.rwlock.RLock()
	C.VectodbSearch(vdb.vdb_c, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	vdb.rwlock.RUnlock()
}

/**
 * Static methods.
 */
func (_ *VectoDB) ClearWorkDir(workDir string) {
	workDir_c := C.CString(workDir)
	C.VectodbClearWorkDir(workDir_c)
	C.free(unsafe.Pointer(workDir_c))
}
