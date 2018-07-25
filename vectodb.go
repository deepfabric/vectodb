package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lboost_thread -lboost_filesystem -lboost_system -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++
// #include "vectodb.h"
// #include <stdlib.h>
import "C"

import (
	"sync"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

type VectoDB struct {
	rwlock        sync.RWMutex
	vdbC          unsafe.Pointer
	workDir       string
	flatThreshold int
}

func NewVectoDB(workDir string, dim int, metricType int, indexKey string, queryParams string, distThreshold float32, flatThreshold int) (vdb *VectoDB, err error) {
	wordDirC := C.CString(workDir)
	indexKeyC := C.CString(indexKey)
	queryParamsC := C.CString(queryParams)
	vdbC := C.VectodbNew(wordDirC, C.long(dim), C.int(metricType), indexKeyC, queryParamsC, C.float(distThreshold))
	vdb = &VectoDB{
		vdbC:          vdbC,
		workDir:       workDir,
		flatThreshold: flatThreshold,
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

func (vdb *VectoDB) AddWithIds(nb int, xb []float32, xids []int64) (err error) {
	C.VectodbAddWithIds(vdb.vdbC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) UpdateWithIds(nb int, xb []float32, xids []int64) (err error) {
	C.VectodbUpdateWithIds(vdb.vdbC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) UpdateIndex() (err error) {
	var needBuild bool
	var index unsafe.Pointer
	var curNtrain, curNsize, ntrain, nflat, played int
	if played, err = vdb.updateBase(); err != nil {
		return
	}
	if played != 0 {
		needBuild = true
		curNtrain = 0
		curNsize = 0
		log.Infof("%s: played %d updates, need build index", vdb.workDir, played)
	} else {
		if nflat, err = vdb.GetFlatSize(); err != nil {
			return
		}
		if nflat >= vdb.flatThreshold {
			needBuild = true
			if curNtrain, curNsize, err = vdb.getIndexSize(); err != nil {
				return
			}
			log.Infof("%s: nflat %d goes above threshold, need build idnex. curNtrain %d, curNsize %d", vdb.workDir, nflat, curNtrain, curNsize)
		}
	}
	if needBuild {
		if index, ntrain, err = vdb.buildIndex(curNtrain, curNsize); err != nil {
			return
		}
		if ntrain != 0 {
			if err = vdb.activateIndex(index, ntrain); err != nil {
				return
			}
		}
		log.Infof("%s: UpdateIndex done", vdb.workDir)
	}
	return
}

func (vdb *VectoDB) buildIndex(cur_ntrain, cur_ntotal int) (index unsafe.Pointer, ntrain int, err error) {
	var ntrainC C.long
	index = C.VectodbBuildIndex(vdb.vdbC, C.long(cur_ntrain), C.long(cur_ntotal), &ntrainC)
	ntrain = int(ntrainC)
	return
}

func (vdb *VectoDB) updateBase() (played int, err error) {
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

func (vdb *VectoDB) activateIndex(index unsafe.Pointer, ntrain int) (err error) {
	vdb.rwlock.Lock()
	defer vdb.rwlock.Unlock()
	C.VectodbActivateIndex(vdb.vdbC, index, C.long(ntrain))
	return
}

/**
 * Reader methods. There could be multiple readers.
 */
func (vdb *VectoDB) getIndexSize() (ntrain, nsize int, err error) {
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

// VectodbCompareDistance returns true if dis1 is closer then dis2.
func VectodbCompareDistance(metricType int, dis1, dis2 float32) bool {
	return (metricType == 0) == (dis1 > dis2)
}
