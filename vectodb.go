package vectodb

// #cgo CFLAGS: -I{SRCDIR} -I{SRCDIR}/faiss
// #cgo CXXFLAGS: -I{SRCDIR} -I{SRCDIR}/faiss
// #cgo LDFLAGS: -lstdc++ -L${SRCDIR} -lvectodb -L${SRCDIR}/faiss -lfaiss
// #include "vectodb.h"

import "C"
