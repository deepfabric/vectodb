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
	"github.com/hashicorp/golang-lru"
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
	h64           hash.Hash64
	expiresCh     chan string
	cancel        context.CancelFunc
}

func NewVectoDBLite(redisAddr string, dbID int, dimIn int, distThreshold float32, sizeLimit int) (vdbl *VectoDBLite, err error) {
	log.Infof("creating VectoDBLite %v %v", redisAddr, dbID)
	dbKey := getDbKey(dbID)
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
		expiresCh:     make(chan string, sizeLimit),
	}
	onEvicted := func(key, value interface{}) {
		vdbl.expiresCh <- key.(string)
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
	log.Debugf("HGetAll(%v): %+v", vdbl.dbKey, vecMapS)
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
		log.Infof("purging expired items %v %v from redis", vdbl.dbKey, expiredXids)
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
			err = errors.Errorf("vdbl.lru is corrupted, want %v be present, have absent", xidInf.(string))
			return
		}
		vt := vtInf.(*VecTimestamp)
		C.IndexFlatAddWithIds(vdbl.flatC, C.long(1), (*C.float)(&vt.Vec[0]), (*C.ulong)(&xid))
	}
	return
}

func (vdbl *VectoDBLite) servExpire(ctx context.Context) {
	tickCh := time.Tick(10 * time.Second)
	expiredXids := make([]string, vdbl.sizeLimit/10)
	for {
		select {
		case <-ctx.Done():
			log.Infof("servExpire goroutine exited")
			return
		case expiredXid := <-vdbl.expiresCh:
			expiredXids = append(expiredXids, expiredXid)
		case <-tickCh:
			if len(expiredXids) == 0 {
				//TODO: remove oldest if it expires?
				continue
			}
			for _, xidS := range expiredXids {
				vdbl.rcli.HDel(vdbl.dbKey, xidS)
				log.Infof("purged %v %v from LRU and redis", vdbl.dbKey, xidS)
			}
		}
	}
}

func (vdbl *VectoDBLite) Destroy() (err error) {
	log.Infof("destroying VectoDBLite %v %v", vdbl.redisAddr, vdbl.dbKey)
	vdbl.cancel()
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
		log.Fatalf("invalid length of xb, want %v, have %v", vdbl.dim, len(xb))
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
	C.IndexFlatAddWithIds(vdbl.flatC, C.long(1), (*C.float)(&xb[0]), (*C.ulong)(&xid))
	return
}

func (vdbl *VectoDBLite) Search(xq []float32) (xid uint64, distance float32, err error) {
	if len(xq) != vdbl.dim {
		log.Fatalf("invalid length of xq, want %v, have %v", vdbl.dim, len(xq))
	}
	C.IndexFlatSearch(vdbl.flatC, C.long(1), (*C.float)(&xq[0]), (*C.float)(&distance), (*C.ulong)(&xid))
	if xid != ^uint64(0) {
		//search ok, update expireAt at lur, and redis.
		xidS := getXidKey(xid)
		var vtInf interface{}
		var ok bool
		if vtInf, ok = vdbl.lru.Get(xidS); !ok {
			log.Infof("xid %v in IndexFlat is absent in LRU", xidS)
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
