package vectodb

// #cgo CXXFLAGS: -I${SRCDIR}
// #cgo LDFLAGS: -L${SRCDIR}/faiss -lfaiss -lopenblas -lgomp -lstdc++ -ljemalloc
// #include "index_flat_wrapper.h"
// #include <stdlib.h>
import "C"

import (
	"context"
	"fmt"
	"hash"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/go-redis/redis"
	"github.com/karlseguin/ccache"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	SIZEOF_FLOAT32       = 4
	ValidSeconds   int64 = 365 * 24 * 60 * 60 // 1 year
	SizeLimit            = 10000
)

// VectoDBLite is tiny stateless non-updatable non-removable vector database. Only supports metric type 0 - METRIC_INNER_PRODUCT.
type VectoDBLite struct {
	redisAddr     string
	dbKey         string
	rcli          *redis.Client
	dim           int
	distThreshold float32
	flatC         unsafe.Pointer
	vecMap        map[string]*VecTimestamp
	lru           *ccache.Cache //The four shall keep sync: redis, flatC, vecMap, lru
	h64           hash.Hash64
	expiresCh     chan *VecTimestamp
	cancel        context.CancelFunc
}

func NewVectoDBLite(redisAddr string, dbID int, dimIn int, distThreshold float32) (vdbl *VectoDBLite, err error) {
	log.Infof("creating VectoDBLite %v %v", redisAddr, dbID)
	dbKey := getDbKey(dbID)
	rcli := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	flatC := C.IndexFlatNew(C.long(dimIn), C.float(distThreshold))
	vecMap := make(map[string]*VecTimestamp)
	lru := ccache.New(ccache.Configure().MaxSize(SizeLimit).ItemsToPrune(1))
	vdbl = &VectoDBLite{
		redisAddr:     redisAddr,
		dbKey:         dbKey,
		rcli:          rcli,
		dim:           dimIn,
		distThreshold: distThreshold,
		flatC:         flatC,
		vecMap:        vecMap,
		lru:           lru,
		h64:           xxhash.New(),
		expiresCh:     make(chan *VecTimestamp, SizeLimit/10),
	}
	return
}

// Init load data from redis
func (vdbl *VectoDBLite) Init() (err error) {
	ctx, cancel := context.WithCancel(context.TODO())
	vdbl.cancel = cancel
	go vdbl.servExpire(ctx)
	vdbl.lru.OnDelete(
		func(item *ccache.Item) {
			value := item.Value()
			if vt, ok := value.(*VecTimestamp); ok {
				vdbl.expiresCh <- vt
			}
		},
	)
	var vecMapS map[string]string
	if vecMapS, err = vdbl.rcli.HGetAll(vdbl.dbKey).Result(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	log.Debugf("HGetAll(%v): %+v", vdbl.dbKey, vecMapS)
	expiredXids := make([]string, 0)
	now := time.Now().Unix()
	for xidS, vtS := range vecMapS {
		vt := VecTimestamp{}
		if err = vt.Unmarshal([]byte(vtS)); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if xidS != vt.Xid {
			err = errors.Errorf("xid doesn't match, want %v have %v", xidS, vt.Xid)
			return
		}
		if vt.ExpireAt < now {
			expiredXids = append(expiredXids, xidS)
		} else {
			vdbl.vecMap[xidS] = &vt
			vdbl.lru.Set(xidS, &vt, time.Second*time.Duration(now-vt.ExpireAt))
		}
	}

	if len(expiredXids) != 0 {
		log.Infof("purging expired items %v %v from redis", vdbl.dbKey, expiredXids)
		for _, xidS := range expiredXids {
			if _, err = vdbl.rcli.HDel(vdbl.dbKey, xidS).Result(); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
	}

	var xid int64
	for _, vt := range vdbl.vecMap {
		if xid, err = strconv.ParseInt(vt.Xid, 16, 64); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		C.IndexFlatAddWithIds(vdbl.flatC, C.long(1), (*C.float)(&vt.Vec[0]), (*C.long)(&xid))
	}

	return
}

func (vdbl *VectoDBLite) servExpire(ctx context.Context) {
	tickCh := time.Tick(10 * time.Second)
	expiredVecs := make([]*ValidSeconds, SizeLimit/10)
	for {
		select {
		case <-ctx.Done():
			log.Infof("servExpire goroutine exited")
			return
		case expiredVec := <-vdbl.expiresCh:
			expiredVecs = append(expiredVecs, expiredVec)
		case <-tickCh:
			if len(expiredVecs) == 0 {
				continue
			}
			for _, vt := range expiredVecs {
				vdbl.rcli.HDel(vdbl.dbKey, vt.Xid)
				delete(vdbl.vecMap, vt.Xid)
				log.Infof("purged %v %v from LRU and redis", vdbl.dbKey, vt.Xid)
			}
			C.IndexFlatDelete(vdbl.flatC)
			vdbl.flatC = C.IndexFlatNew(C.long(vdbl.dim), C.float(vdbl.distThreshold))
			var xid int64
			var err error
			for _, vt := range vdbl.vecMap {
				if xid, err = strconv.ParseInt(vt.Xid, 16, 64); err != nil {
					err = errors.Wrapf(err, "")
					log.Errorf("got error %+v", err)
					continue
				}
				C.IndexFlatAddWithIds(vdb.flatC, C.long(1), (*C.float)(&vt.Vec[0]), (*C.long)(&xid))
			}
		}
	}
}

func (vdbl *VectoDBLite) Destroy() (err error) {
	log.Infof("destroying VectoDBLite %v %v", vdbl.redisAddr, vdbl.dbKey)
	C.IndexFlatDelete(vdbl.flatC)
	vdbl.flatC = nil
	return
}

func (vdbl *VectoDBLite) Add(xb []float32) (err error) {
	if len(xb) != vdbl.dim {
		log.Fatalf("invalid length of xb, want %v, have %v", vdbl.dim, len(xb))
	}
	xid := allocateXid(xb)
	xidS := getXidKey(xid)
	vt := &VecTimestamp{
		Xid:      xidS,
		Vec:      xb,
		ExpireAt: time.Now().Unix() + ValidSeconds,
	}
	var vtB []byte
	if vtB, err = vt.Marshal(); err != nil {
		err = errors.Wrapf(err, "")
		return
	}

	if _, err = vdbl.rcli.HSet(vdbl.dbKey, xidS, string(vtB)).Result(); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	vdbl.vecMap[xidS] = vt
	vdbl.lru.Set(xidS, vt, time.Duration(ValidSeconds)*time.Second)
	C.IndexFlatAddWithIds(vdbl.flatC, C.long(1), (*C.float)(&xb[0]), (*C.long)(&xid))
	return
}

func (vdbl *VectoDBLite) Search(xq []float32, distances []float32, xids []int64) (err error) {
	nq := len(xids)
	if len(xq) != nq*vdbl.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", nq*vdbl.dim, len(xq))
	}
	if len(distances) != nq {
		log.Fatalf("invalid length of distances, want %v, have %v", nq, len(distances))
	}
	C.IndexFlatSearch(vdbl.flatC, C.long(nq), (*C.float)(&xq[0]), (*C.float)(&distances[0]), (*C.long)(&xids[0]))
	return
}

func getXidKey(xid int64) string {
	return fmt.Sprintf("%016x", xid)
}

func getDbKey(dbID int) string {
	return fmt.Sprintf("vectodblite_%v", dbID)
}

// allocateXid uses hash of vec as xid.
func allocateXid(h64 xxhash.Hash64, vec []float32) (xid int64) {
	// https://stackoverflow.com/questions/11924196/convert-between-slices-of-different-types
	// Get the slice header
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&vec))
	// The length and capacity of the slice are different.
	header.Len *= SIZEOF_FLOAT32
	header.Cap *= SIZEOF_FLOAT32
	// Convert slice header to an []byte
	data := *(*[]byte)(unsafe.Pointer(&header))

	h64.Reset()
	h64.Write(data)
	xid = int64(this.h64.Sum64())
	return
}
