package vectodb

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

/**
 * VectodbMulti consists of multiple VectoDB instances on the same machine.
 * It's the POC of the VectoDB cluster which involves multiple machines.
 */
type VectodbMulti struct {
	//configurations
	dim         int
	metricType  int
	indexKey    string
	queryParams string
	workDir     string //the working directory of each VectoDB instance is <workDir>/vdb-<seq>
	sizeLimit   int    //size limit of each VectoDB instance

	//state
	curXidBatch int64
	maxSeq      int
	vdbs        []*VectoDB
	cancel      context.CancelFunc
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

//CompareDistance returns true if dis1 is bigger than dis2 per metricType (0 - IP, 1 - L2).
func CompareDistance(metricType int, dis1, dis2 float32) (le bool) {
	if metricType == 1 {
		return dis1 > dis2
	} else {
		return dis1 < dis2
	}
}

func getWorkDir(seq int) string {
	return fmt.Sprintf("vdb-%d", seq)
}

func VectodbMultiClearWorkDir(workDir string) (err error) {
	re := regexp.MustCompile("vdb-(?P<seq>[0-9]+)")

	entries, err := ioutil.ReadDir(workDir)
	if err != nil && !os.IsNotExist(err) {
		err = errors.Wrap(err, "")
		return
	}

	for _, entry := range entries {
		subs := re.FindStringSubmatch(entry.Name())
		if subs == nil {
			continue
		}
		dp := filepath.Join(workDir, subs[0])
		if err = os.RemoveAll(dp); err != nil {
			return
		}
	}
	return
}

func NewVectodbMulti(workDir string, dim int, metricType int, indexKey string, queryParams string, sizeLimit int) (vm *VectodbMulti, err error) {
	vm = &VectodbMulti{
		dim:         dim,
		metricType:  metricType,
		indexKey:    indexKey,
		queryParams: queryParams,
		workDir:     workDir,
		sizeLimit:   sizeLimit,
		curXidBatch: 0,
	}
	if err = os.MkdirAll(workDir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}

	seqs := make([]int, 0)
	re := regexp.MustCompile("vdb-(?P<seq>[0-9]+)")

	entries, err := ioutil.ReadDir(workDir)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}

	var seq int
	for _, entry := range entries {
		subs := re.FindStringSubmatch(entry.Name())
		if subs == nil {
			continue
		}
		if seq, err = strconv.Atoi(subs[1]); err != nil {
			return
		}
		seqs = append(seqs, seq)
	}
	var vdb *VectoDB
	if len(seqs) == 0 {
		seqs = append(seqs, 0)
	}
	sort.Ints(seqs)
	for _, seq := range seqs {
		dp := filepath.Join(workDir, getWorkDir(seq))
		vdb, err = NewVectoDB(dp, dim, metricType, indexKey, queryParams)
		vm.vdbs = append(vm.vdbs, vdb)
	}
	vm.maxSeq = seqs[len(seqs)-1]
	return
}

//Search perform batch search
/**
 * disThr	distance threshold
 * nq       number of query points, shall be equal to len(xids)
 * xq       query points
 * xids     vector identifiers
 */
func (vm *VectodbMulti) Search(disThr float32, nq int, xq []float32) (xids []int64, err error) {
	dis := make([]float32, nq)
	xids = make([]int64, nq)
	for i := 0; i < nq; i++ {
		dis[i] = disThr
		xids[i] = int64(-1)
	}
	dis2 := make([]float32, nq)
	xids2 := make([]int64, nq)
	var total int
	for _, vdb := range vm.vdbs {
		for i := 0; i < nq; i++ {
			dis2[i] = disThr
		}
		if total, err = vdb.Search(nq, xq, dis2, xids2); err != nil {
			return
		}
		if total == 0 {
			continue
		}
		for i := 0; i < nq; i++ {
			if CompareDistance(vm.metricType, dis[i], dis2[i]) {
				dis[i] = dis2[i]
				xids[i] = xids2[i]
			}
		}
	}
	return
}

//AddWithIds add vectors
/**
 * nb       number of vectors, shall be equal to len(xids)
 * xb       vectors
 * xids     vector identifiers
 */
func (vm *VectodbMulti) AddWithIds(nb int, xb []float32, xids []int64) (err error) {
	var quota, total, added int
	vdb := vm.vdbs[len(vm.vdbs)-1]
	for added < nb {
		if total, err = vdb.GetTotal(); err != nil {
			return
		}
		quota = vm.sizeLimit - total
		if quota > 0 {
			batch := MinInt(quota, nb-added)
			if err = vdb.AddWithIds(batch, xb[added*vm.dim:], xids[added:]); err != nil {
				return
			}
			added += batch
		} else {
			vm.maxSeq++
			dp := filepath.Join(vm.workDir, getWorkDir(vm.maxSeq))
			if vdb, err = NewVectoDB(dp, vm.dim, vm.metricType, vm.indexKey, vm.queryParams); err != nil {
				return
			}
			vm.vdbs = append(vm.vdbs, vdb)
		}

	}
	return
}

//UpdateWithIds update vectors
/**
 * nb       number of vectors, shall be equal to len(xids)
 * xb       vectors
 * xids     vector identifiers
 */
func (vm *VectodbMulti) UpdateWithIds(nb int, xb []float32, xids []int64) (err error) {
	for _, vdb := range vm.vdbs {
		if err = vdb.UpdateWithIds(nb, xb, xids); err != nil {
			return
		}
	}
	return
}

//StartBuilderLoop starts a goroutine to build build index in loop
func (vm *VectodbMulti) StartBuilderLoop() {
	if vm.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.Tick(2 * time.Second)
		threshold := vm.sizeLimit / 200
		var index unsafe.Pointer
		var cur_ntrain, cur_nsize, ntrain, nflat int
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker:
				log.Printf("build iteration begin")
				vdbs := vm.vdbs
				for _, vdb := range vdbs {
					if nflat, err = vdb.GetFlatSize(); err != nil {
						log.Fatalf("%+v", err)
						continue
					}
					if nflat >= threshold {
						if cur_ntrain, cur_nsize, err = vdb.GetIndexSize(); err != nil {
							log.Fatalf("%+v", err)
							continue
						}
						log.Printf("cur_ntrain %d, cur_nsize %d", cur_ntrain, cur_nsize)
						if index, ntrain, err = vdb.BuildIndex(cur_ntrain, cur_nsize); err != nil {
							log.Fatalf("%+v", err)
							continue
						}
						log.Printf("BuildIndex done")
						if ntrain != 0 {
							if err = vdb.ActivateIndex(index, ntrain); err != nil {
								log.Fatalf("%+v", err)
								continue
							}
						}
					}

					// sleep a while to avoid busy loop
					select {
					case <-ctx.Done():
						return
					case <-ticker:
					}
				}
				log.Printf("build iteration done")
			}
		}

	}()
	vm.cancel = cancel
	return
}

//StopBuilderLoop stops the build goroutinep
func (vm *VectodbMulti) StopBuilderLoop() {
	if vm.cancel == nil {
		return
	}
	vm.cancel()
	vm.cancel = nil
	return
}

//AllocateIds allocate a batch of identifiers. The batch size is 2<<20.
func (vm *VectodbMulti) AllocateIds() (xidBegin int64, err error) {
	xidBatch := atomic.AddInt64(&vm.curXidBatch, int64(1)) - 1
	xidBegin = xidBatch * int64(2<<20)
	return
}
