package vectodb

// https://golang.org/cmd/cgo/
// When the Go tool sees that one or more Go files use the special import "C", it will look for other non-Go files in the directory and compile them as part of the Go package.

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lboost_thread -lboost_filesystem -lboost_system -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++ -ljemalloc
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

func NewVectoDB(workDir string, dimIn int) (vdb *VectoDB, err error) {
	log.Infof("creating VectoDB %v", workDir)
	wordDirC := C.CString(workDir)
	vdbC := C.VectodbNew(wordDirC, C.long(dimIn))
	vdb = &VectoDB{
		vdbC:    vdbC,
		dim:     dimIn,
		workDir: workDir,
	}
	C.free(unsafe.Pointer(wordDirC))
	return
}

func (vdb *VectoDB) Destroy() (err error) {
	log.Infof("destroying VectoDB %+v", vdb)
	C.VectodbDelete(vdb.vdbC)
	vdb.vdbC = nil
	return
}

/*
input parameters:
@param xb:   nb个向量
@param xids: nb个向量编号。xid 64 bit结构：高32 bit为uid（用户ID），低32 bit为pid（图片ID）
*/
func (vdb *VectoDB) AddWithIds(xb []float32, xids []int64) (err error) {
	nb := len(xids)
	if len(xb) != nb*vdb.dim {
		log.Fatalf("invalid length of xb, want %v, have %v", nb*vdb.dim, len(xb))
	}
	C.VectodbAddWithIds(vdb.vdbC, C.long(nb), (*C.float)(&xb[0]), (*C.long)(&xids[0]))
	return
}

func (vdb *VectoDB) UpdateIndex() (err error) {
	var needBuild bool
	var index unsafe.Pointer
	var curNtrain, curNsize, ntrain, nflat int
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

/**
input parameters:
@param xq:      nq个查询向量
@param ks:      nq个参数k（每个kNN查询中的参数k）
@param uids:    nq个序列化的roaring bitmap

output parameters:
@param scores:  所有结果的得分（查询1的k个得分，查询2的k个得分,...）
@param xids:    所有结果的向量编号（查询1的k个向量编号，查询2的k个向量编号,...）

return parameters:
@return ntotal  数据库当前存储的向量总数
@return err     错误
*/
func (vdb *VectoDB) Search(xq []float32, ks []int64, uids []string, scores []float32, xids []int64) (ntotal int, err error) {
	nq := len(ks)
	if len(xq) != nq*vdb.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", nq*vdb.dim, len(xq))
	}
	if len(uids) != nq {
		log.Fatalf("invalid length of uids, want %v, have %v", nq, len(uids))
	}
	var sum_k int
	for i, k := range ks {
		if k <= 0 {
			log.Fatalf("invalid ks[%v], want >0, have %v", i, ks[i])
		}
		sum_k += int(k)
	}
	if len(scores) < sum_k {
		log.Fatalf("invalid length of scores, want >=%v, have %v", sum_k, len(scores))
	}
	if len(xids) != len(scores) {
		log.Fatalf("invalid length of xids, want len(scores)=%v, have %v", len(scores), len(scores))
	}
	uidsFilter := make([]int64, nq)
	for i := 0; i < nq; i++ {
		uidsFilter[i] = &uids[i][0]
	}
	ntotalC := C.VectodbSearch(vdb.vdbC, C.long(nq), (*C.float)(&xq[0]), (*C.long)(&ks[0]), (*C.long)(&uidsFilter[0]), (*C.float)(&scores[0]), (*C.long)(&xids[0]))
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
