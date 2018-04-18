package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lboost_filesystem -lboost_system -lglog -lgflags -lfaiss -lopenblas -lgomp -lstdc++
// #include "vectodb.h"
// #include <stdlib.h>
import "C"
import "unsafe"

type VectoDB struct {
	vdb_c unsafe.Pointer
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
