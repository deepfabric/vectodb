package vectodb

// https://golang.org/cmd/cgo/
// When the Go tool sees that one or more Go files use the special import "C", it will look for other non-Go files in the directory and compile them as part of the Go package.

// #cgo CXXFLAGS: -std=c++17 -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++ -lstdc++fs -ljemalloc
// #include "vectodb.h"
// #include <stdlib.h>
import "C"

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

const small_set_size int = 32

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
func (vdb *VectoDB) Search(k int, top_vectors bool, xq []float32, uids [][]byte) (res [][]XidScore, err error) {
	nq := len(xq) / vdb.dim
	if len(xq) != nq*vdb.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", nq*vdb.dim, len(xq))
	}
	if uids != nil && len(uids) != nq {
		log.Fatalf("invalid length of uids, want nil or %v, have %v", nq, len(uids))
	}
	res = make([][]XidScore, nq)
	scores := make([]float32, nq*k)
	xids := make([]int64, nq*k)
	var p_uids **uint8
	if uids != nil {
		uids2 := make([]*uint8, nq)
		for i := 0; i < nq; i++ {
			if uids[i] != nil && len(uids[i]) > 0 {
				uids2[i] = &uids[i][0]
			} else {
				uids2[i] = nil
			}
		}
		p_uids = &uids2[0]
	}
	vector_or_user := int(0)
	if top_vectors {
		vector_or_user = 1
	}
	C.VectodbSearch(vdb.vdbC, C.long(nq), (*C.float)(&xq[0]), C.long(k), C.int(vector_or_user), (*C.long)(unsafe.Pointer(p_uids)), (*C.float)(&scores[0]), (*C.long)(&xids[0]))
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

// Keep sync with vectodb.hpp, vectodb.cpp
func GetUid(xid uint64) uint64      { return xid >> 34 }
func GetPid(xid uint64) uint64      { return xid & 0x3FFFFFFFF }
func GetXid(uid, pid uint64) uint64 { return (uid << 34) + pid }

// Keep sync with RoaringBitmapWithSmallSet in Clickhouse
func ChBitmapSerialize(rb *roaring.Bitmap) (buf []byte, err error) {
	num := int(rb.GetCardinality())
	buf2 := make([]byte, 9)
	if num <= small_set_size {
		written := binary.PutUvarint(buf2, uint64(num))
		buf = make([]byte, 1+written+4*num)
		buf[0] = byte(0)
		copy(buf[1:1+written], buf2)
		off := 1 + written
		values := rb.ToArray()
		for i := 0; i < num; i++ {
			binary.LittleEndian.PutUint32(buf[off:], values[i])
			off += 4
		}
	} else {
		var buf3 []byte
		if buf3, err = rb.MarshalBinary(); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		size := len(buf3)
		written := binary.PutUvarint(buf2, uint64(size))
		buf = make([]byte, 1+written+size)
		buf[0] = byte(1)
		copy(buf[1:1+written], buf2)
		copy(buf[1+written:], buf3)
	}
	return
}

func ChBitmapDeserialize(buf []byte) (rb *roaring.Bitmap, err error) {
	isSmall := (0x0 == buf[0])
	num, readed := binary.Uvarint(buf[1:])
	if readed <= 0 {
		err = errors.Errorf("Failed to decode uvarint from %v", buf[1:10])
		return
	}
	off := 1 + readed
	rb = roaring.New()
	if isSmall {
		for i := 0; i < int(num); i++ {
			val := binary.LittleEndian.Uint32(buf[off : off+4])
			rb.Add(val)
			off += 4
		}
	} else {
		err = rb.UnmarshalBinary(buf[off:])
	}
	return
}
