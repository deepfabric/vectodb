package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/xxHash -L${SRCDIR}/faiss -lboost_thread -lboost_filesystem -lboost_system -lglog -lgflags -lxxhash -lfaiss -lopenblas -lgomp -lstdc++ -ljemalloc
// #include "vectodb.h"
// #include <stdlib.h>
import "C"

import (
	"unsafe"

	log "github.com/sirupsen/logrus"
)

type VectoDB struct {
	vdbC          unsafe.Pointer
	dim           int
	workDir       string
	flatThreshold int
}

func NewVectoDB(workDir string, dimIn int, metricType int, indexKey string, queryParams string, distThreshold float32, flatThreshold int) (vdb *VectoDB, err error) {
	log.Infof("creating VectoDB %v", workDir)
	wordDirC := C.CString(workDir)
	indexKeyC := C.CString(indexKey)
	queryParamsC := C.CString(queryParams)
	vdbC := C.VectodbNew(wordDirC, C.long(dimIn), C.int(metricType), indexKeyC, queryParamsC, C.float(distThreshold))
	vdb = &VectoDB{
		vdbC:          vdbC,
		dim:           dimIn,
		workDir:       workDir,
		flatThreshold: flatThreshold,
	}
	C.free(unsafe.Pointer(wordDirC))
	C.free(unsafe.Pointer(indexKeyC))
	C.free(unsafe.Pointer(queryParamsC))
	return
}

func (vdb *VectoDB) GetNextXid() (nextXid int) {
	nextXidC := C.VectodbGetNextXid(vdb.vdbC)
	nextXid = int(nextXidC)
	return
}

func (vdb *VectoDB) SetNextXid(nextXid int) (effNextXid int) {
	effNextXidC := C.VectodbSetNextXid(vdb.vdbC, C.long(nextXid))
	effNextXid = int(effNextXidC)
	return
}

func (vdb *VectoDB) Destroy() (err error) {
	log.Infof("destroying VectoDB %+v", vdb)
	C.VectodbDelete(vdb.vdbC)
	vdb.vdbC = nil
	return
}

func (vdb *VectoDB) AddWithIds(xb []float32, xids []int64) (err error) {
	nb := len(xids)
	if len(xb) != nb*vdb.dim {
		log.Fatalf("invalid length of xb, want %v, have %v", nb*vdb.dim, len(xb))
	}
	C.VectodbAddWithIds(vdb.vdbC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) UpdateWithIds(xb []float32, xids []int64) (err error) {
	nb := len(xids)
	if len(xb) != nb*vdb.dim {
		log.Fatalf("invalid length of xb, want %v, have %v", nb*vdb.dim, len(xb))
	}
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

func (vdb *VectoDB) activateIndex(index unsafe.Pointer, ntrain int) (err error) {
	C.VectodbActivateIndex(vdb.vdbC, index, C.long(ntrain))
	return
}

func (vdb *VectoDB) getIndexSize() (ntrain, nsize int, err error) {
	var ntrainC, nsizeC C.long
	C.VectodbGetIndexSize(vdb.vdbC, &ntrainC, &nsizeC)
	ntrain = int(ntrainC)
	nsize = int(nsizeC)
	return
}

func (vdb *VectoDB) Search(xq []float32, distances []float32, xids []int64) (ntotal int, err error) {
	nq := len(xids)
	if len(xq) != nq*vdb.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", nq*vdb.dim, len(xq))
	}
	if len(distances) != nq {
		log.Fatalf("invalid length of distances, want %v, have %v", nq, len(distances))
	}
	ntotalC := C.VectodbSearch(vdb.vdbC, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	ntotal = int(ntotalC)
	return
}

/**
 * Static methods.
 */

func VectodbClearWorkDir(workDir string) (err error) {
	log.Infof("clearing VectoDB %v", workDir)
	wordDirC := C.CString(workDir)
	C.VectodbClearWorkDir(wordDirC)
	C.free(unsafe.Pointer(wordDirC))
	return
}

// VectodbCompareDistance returns true if dis1 is closer then dis2.
func VectodbCompareDistance(metricType int, dis1, dis2 float32) bool {
	return (metricType == 0) == (dis1 > dis2)
}
