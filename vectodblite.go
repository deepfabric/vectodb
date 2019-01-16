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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/go-redis/redis"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	SIZEOF_FLOAT32       = 4
	ValidSeconds   int64 = 365 * 24 * 60 * 60 // 1 year
)

// VectoDBLite is tiny stateless non-updatable non-removable vector database. Only supports metric type 0 - METRIC_INNER_PRODUCT.
type VectoDBLite struct {
	redisAddr     string
	dim           int
	distThreshold float32
	sizeLimit     int
	dbKey         string
	rcli          *redis.Client
	lru           *lru.Cache //The three shall keep sync: redis, lru, flatC
	flatC         unsafe.Pointer
	rwlock        sync.RWMutex // protect flatC
	h64           hash.Hash64
	numEvicted    int32
	cancel        context.CancelFunc
}

func NewVectoDBLite(redisAddr string, dbID int, dimIn int, distThreshold float32, sizeLimit int) (vdbl *VectoDBLite, err error) {
	dbKey := getDbKey(dbID)
	log.Infof("vectodblite %s creating", dbKey)
	rcli := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	vdbl = &VectoDBLite{
		redisAddr:     redisAddr,
		dim:           dimIn,
		distThreshold: distThreshold,
		sizeLimit:     sizeLimit,
		dbKey:         dbKey,
		rcli:          rcli,
		h64:           xxhash.New(),
	}
	onEvicted := func(key, value interface{}) {
		xidS := key.(string)
		vdbl.rcli.HDel(vdbl.dbKey, xidS)
		atomic.AddInt32(&vdbl.numEvicted, int32(1))
	}
	if vdbl.lru, err = lru.NewWithEvict(sizeLimit, onEvicted); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	ctx, cancel := context.WithCancel(context.TODO())
	vdbl.cancel = cancel
	go vdbl.servExpire(ctx)
	if err = vdbl.load(); err != nil {
		return
	}
	return
}

// Init load data from redis
func (vdbl *VectoDBLite) load() (err error) {
	var vecMapS map[string]string
	if vecMapS, err = vdbl.rcli.HGetAll(vdbl.dbKey).Result(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	log.Debugf("vectodblite %s HGetAll: %+v", vdbl.dbKey, vecMapS)
	expiredXids := make([]string, 0)
	now := time.Now().Unix()
	for xidS, vtS := range vecMapS {
		vt := VecTimestamp{}
		if err = vt.Unmarshal([]byte(vtS)); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if vt.ExpireAt < now {
			expiredXids = append(expiredXids, xidS)
		} else {
			vdbl.lru.Add(xidS, &vt)
		}
	}

	if len(expiredXids) != 0 {
		log.Infof("vectodblite %s purging expired items from redis: %v", vdbl.dbKey, expiredXids)
		for _, xidS := range expiredXids {
			if _, err = vdbl.rcli.HDel(vdbl.dbKey, xidS).Result(); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
	}

	if err = vdbl.rebuildFlatC(); err != nil {
		return
	}

	return
}

func (vdbl *VectoDBLite) rebuildFlatC() (err error) {
	vdbl.rwlock.Lock()
	defer vdbl.rwlock.Unlock()
	if vdbl.flatC != nil {
		C.IndexFlatDelete(vdbl.flatC)
	}
	vdbl.flatC = C.IndexFlatNew(C.long(vdbl.dim), C.float(vdbl.distThreshold))
	var xid uint64
	for _, xidInf := range vdbl.lru.Keys() {
		if xid, err = strconv.ParseUint(xidInf.(string), 16, 64); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		var vtInf interface{}
		var ok bool
		if vtInf, ok = vdbl.lru.Peek(xidInf); !ok {
			err = errors.Errorf("vectodblite %s vdbl.lru is corrupted, want %v be present, have absent", vdbl.dbKey, xidInf.(string))
			return
		}
		vt := vtInf.(*VecTimestamp)
		C.IndexFlatAddWithIds(vdbl.flatC, C.long(1), (*C.float)(&vt.Vec[0]), (*C.ulong)(&xid))
	}
	return
}

func (vdbl *VectoDBLite) servExpire(ctx context.Context) {
	tickCh := time.Tick(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Infof("vectodblite %s servExpire goroutine exited", vdbl.dbKey)
			return
		case <-tickCh:
			if atomic.SwapInt32(&vdbl.numEvicted, 0) != 0 {
				if err := vdbl.rebuildFlatC(); err != nil {
					log.Errorf("vectodblite %s got error %+v", vdbl.dbKey, err)
				}
			}
		}
	}
}

func (vdbl *VectoDBLite) Destroy() (err error) {
	log.Infof("vectodblite %s destroying", vdbl.dbKey)
	vdbl.cancel()
	vdbl.rwlock.Lock()
	defer vdbl.rwlock.Unlock()
	if vdbl.flatC != nil {
		C.IndexFlatDelete(vdbl.flatC)
		vdbl.flatC = nil
	}
	return
}

func (vdbl *VectoDBLite) Add(xb []float32) (xid uint64, err error) {
	xid = allocateXid(vdbl.h64, xb)
	if err = vdbl.AddWithId(xb, xid); err != nil {
		return
	}
	return
}

func (vdbl *VectoDBLite) AddWithId(xb []float32, xid uint64) (err error) {
	if len(xb) != vdbl.dim {
		err = errors.Errorf("vectodblite %s invalid length of xb, want %v, have %v", vdbl.dbKey, vdbl.dim, len(xb))
		return
	}
	xidS := getXidKey(xid)
	vt := &VecTimestamp{
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
	vdbl.lru.Add(xidS, vt)
	vdbl.rwlock.Lock()
	C.IndexFlatAddWithIds(vdbl.flatC, C.long(1), (*C.float)(&xb[0]), (*C.ulong)(&xid))
	vdbl.rwlock.Unlock()
	return
}

func (vdbl *VectoDBLite) Search(xq []float32) (xid uint64, distance float32, err error) {
	if len(xq) != vdbl.dim {
		err = errors.Errorf("vectodblite %s invalid length of xq, want %v, have %v", vdbl.dbKey, vdbl.dim, len(xq))
		return
	}
	vdbl.rwlock.RLock()
	C.IndexFlatSearch(vdbl.flatC, C.long(1), (*C.float)(&xq[0]), (*C.float)(&distance), (*C.ulong)(&xid))
	vdbl.rwlock.RUnlock()
	if xid != ^uint64(0) {
		//search ok, update expireAt at lur, and redis.
		xidS := getXidKey(xid)
		var vtInf interface{}
		var ok bool
		if vtInf, ok = vdbl.lru.Get(xidS); !ok {
			log.Infof("vectodblite %s xid %v in IndexFlat is absent in LRU", vdbl.dbKey, xidS)
			xid = ^uint64(0)
			return
		}
		vt := vtInf.(*VecTimestamp)
		vt.ExpireAt = time.Now().Unix() + ValidSeconds
		var vtB []byte
		if vtB, err = vt.Marshal(); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if _, err = vdbl.rcli.HSet(vdbl.dbKey, xidS, string(vtB)).Result(); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}

func (vdbl *VectoDBLite) Size() int {
	return vdbl.lru.Len()
}

func getXidKey(xid uint64) string {
	return fmt.Sprintf("%016x", xid)
}

func getDbKey(dbID int) string {
	return fmt.Sprintf("vectodblite_%v", dbID)
}

// allocateXid uses hash of vec as xid.
func allocateXid(h64 hash.Hash64, vec []float32) (xid uint64) {
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
	xid = h64.Sum64()
	return
}
