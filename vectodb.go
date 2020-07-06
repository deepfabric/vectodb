package vectodb

// https://golang.org/cmd/cgo/
// When the Go tool sees that one or more Go files use the special import "C", it will look for other non-Go files in the directory and compile them as part of the Go package.

// #cgo CXXFLAGS: -std=c++17 -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++ -lstdc++fs -ljemalloc
// #include "vectodb.h"
// #include <stdlib.h>
import "C"

import (
	"math"
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

/** removes IDs from the index. Returns the number of elements removed.
 */
func (vdb *VectoDB) RemoveIds(xids []int64) (nremove int) {
	nb := len(xids)
	nremoveC := C.VectodbRemoveIds(vdb.vdbC, C.long(nb), (*C.long)(&xids[0]))
	nremove = int(nremoveC)
	return
}

/**
* Removes all elements from the database.
 */
func (vdb *VectoDB) Reset() {
	C.VectodbReset(vdb.vdbC)
}

/**
* Returns the number of elements in the database.
 */
func (vdb *VectoDB) GetTotal() (total int, err error) {
	totalC := C.VectodbGetTotal(vdb.vdbC)
	total = int(totalC)
	return
}

type XidScore struct {
	Xid   int64
	Score float32
}

/**
input parameters:
@param ks:      kNN参数k
@param xq:      nq个查询向量
@param uids:    nq个序列化的roaring bitmap

output parameters:
@param scores:  所有结果的得分（查询1的k个得分，查询2的k个得分,...）
@param xids:    所有结果的向量编号（查询1的k个向量编号，查询2的k个向量编号,...）

return parameters:
@return err     错误
*/
func (vdb *VectoDB) Search(k int, xq []float32, uids []string) (res [][]XidScore, err error) {
	nq := len(xq) / vdb.dim
	if len(xq) != nq*vdb.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", nq*vdb.dim, len(xq))
	}
	if len(uids) != nq {
		log.Fatalf("invalid length of uids, want %v, have %v", nq, len(uids))
	}
	res = make([][]XidScore, nq)
	scores := make([]float32, nq*k)
	xids := make([]int64, nq*k)
	var uidsFilter int64
	C.VectodbSearch(vdb.vdbC, C.long(nq), (*C.float)(&xq[0]), C.long(k), (*C.long)(&uidsFilter), (*C.float)(&scores[0]), (*C.long)(&xids[0]))
	for i := 0; i < nq; i++ {
		for j := 0; j < k; j++ {
			if xids[i*k+j] == int64(-1) {
				break
			}
			res[i] = append(res[i], XidScore{Xid: xids[i*k+j], Score: scores[i*k+j]})
		}
	}
	return
}

/**
 * Static methods.
 */

func VectodbClearWorkDir(workDir string) (err error) {
	log.Infof("clearing VectoDB %v", workDir)
	wordDirC := C.CString(workDir)
	C.VectodbClearDir(wordDirC)
	C.free(unsafe.Pointer(wordDirC))
	return
}

func NormalizeVec(d int, v []float32) {
	var norm float64
	for i := 0; i < d; i++ {
		norm += float64(v[i]) * float64(v[i])
	}
	norm = math.Sqrt(norm)
	for i := 0; i < d; i++ {
		v[i] = float32(float64(v[i]) / norm)
	}
}
