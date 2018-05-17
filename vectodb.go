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
	vdbC   unsafe.Pointer
}

func NewVectoDB(workDir string, dim int, metricType int, indexKey string, queryParams string) (vdb *VectoDB, err error) {
	wordDirC := C.CString(workDir)
	indexKeyC := C.CString(indexKey)
	queryParamsC := C.CString(queryParams)
	vdbC := C.VectodbNew(wordDirC, C.long(dim), C.int(metricType), indexKeyC, queryParamsC)
	vdb = &VectoDB{
		vdbC: vdbC,
	}
	C.free(unsafe.Pointer(wordDirC))
	C.free(unsafe.Pointer(indexKeyC))
	C.free(unsafe.Pointer(queryParamsC))
	return
}

func (vdb *VectoDB) Destroy() (err error) {
	C.VectodbDelete(vdb.vdbC)
	return
}

func (vdb *VectoDB) BuildIndex(cur_ntrain, cur_ntotal int) (index unsafe.Pointer, ntrain int, err error) {
	var ntrainC C.long
	index = C.VectodbBuildIndex(vdb.vdbC, C.long(cur_ntrain), C.long(cur_ntotal), &ntrainC)
	ntrain = int(ntrainC)
	return
}

func (vdb *VectoDB) AddWithIds(nb int, xb []float32, xids []int64) (err error) {
	C.VectodbAddWithIds(vdb.vdbC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) UpdateWithIds(nb int, xb []float32, xids []int64) (err error) {
	C.VectodbUpdateWithIds(vdb.vdbC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) UpdateBase() (played int, err error) {
	playedC := C.VectodbUpdateBase(vdb.vdbC)
	played = int(playedC)
	return
}

func (vdb *VectoDB) GetTotal() (total int, err error) {
	totalC := C.VectodbGetFlatSize(vdb.vdbC)
	total = int(totalC)
	return
}

func (vdb *VectoDB) GetFlatSize() (nsize int, err error) {
	nsizeC := C.VectodbGetFlatSize(vdb.vdbC)
	nsize = int(nsizeC)
	return
}

/**
 * Writer methods. There could be multiple writers.
 */

func (vdb *VectoDB) ActivateIndex(index unsafe.Pointer, ntrain int) (err error) {
	vdb.rwlock.Lock()
	defer vdb.rwlock.Unlock()
	C.VectodbActivateIndex(vdb.vdbC, index, C.long(ntrain))
	return
}

/**
 * Reader methods. There could be multiple readers.
 */
func (vdb *VectoDB) GetIndexSize() (ntrain, nsize int, err error) {
	vdb.rwlock.RLock()
	defer vdb.rwlock.RUnlock()
	var ntrainC, nsizeC C.long
	C.VectodbGetIndexSize(vdb.vdbC, &ntrainC, &nsizeC)
	ntrain = int(ntrainC)
	nsize = int(nsizeC)
	return
}

func (vdb *VectoDB) Search(nq int, xq []float32, distances []float32, xids []int64) (ntotal int, err error) {
	vdb.rwlock.RLock()
	ntotalC := C.VectodbSearch(vdb.vdbC, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	vdb.rwlock.RUnlock()
	ntotal = int(ntotalC)
	return
}

/**
 * Static methods.
 */

func VectodbClearWorkDir(workDir string) (err error) {
	wordDirC := C.CString(workDir)
	C.VectodbClearWorkDir(wordDirC)
	C.free(unsafe.Pointer(wordDirC))
	return
}

func VectodbCompareDistance(metricType int, dis1, dis2 float32) (which int) {
	whichC := C.VectodbCompareDistance(C.int(metricType), C.float(dis1), C.float(dis2))
	which = int(whichC)
	return
}
