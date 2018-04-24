package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lboost_thread -lboost_filesystem -lboost_system -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++
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

func (vdb *VectoDB) AddWithIds(nb int, xb []float32, xids []int64) (err error) {
	C.VectodbAddWithIds(vdb.vdb_c, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) GetFlatSize() (nsize int, err error) {
	nsize_c := C.VectodbGetFlatSize(vdb.vdb_c)
	nsize = int(nsize_c)
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

/**
 * Reader methods. There could be multiple readers.
 */
func (vdb *VectoDB) GetIndexSize() (ntrain, nsize int, err error) {
	vdb.rwlock.RLock()
	defer vdb.rwlock.RUnlock()
	var ntrain_c, nsize_c C.long
	C.VectodbGetIndexSize(vdb.vdb_c, &ntrain_c, &nsize_c)
	ntrain = int(ntrain_c)
	nsize = int(nsize_c)
	return
}

func (vdb *VectoDB) Search(nq int, xq []float32, distances []float32, xids []int64) (ntotal int, err error) {
	vdb.rwlock.RLock()
	ntotal_c := C.VectodbSearch(vdb.vdb_c, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	vdb.rwlock.RUnlock()
	ntotal = int(ntotal_c)
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
