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
	distThr     float32
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
	err = nil

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

func NewVectodbMulti(workDir string, dim int, metricType int, indexKey string, queryParams string, distThr float32, sizeLimit int) (vm *VectodbMulti, err error) {
	vm = &VectodbMulti{
		dim:         dim,
		metricType:  metricType,
		indexKey:    indexKey,
		queryParams: queryParams,
		distThr:     distThr,
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
		vdb, err = NewVectoDB(dp, dim, metricType, indexKey, queryParams, distThr, vm.sizeLimit/200)
		vm.vdbs = append(vm.vdbs, vdb)
	}
	vm.maxSeq = seqs[len(seqs)-1]
	return
}

//Search perform batch search
/**
 * nq       number of query points, shall be equal to len(xids)
 * xq       query points
 * xids     vector identifiers
 */
func (vm *VectodbMulti) Search(xq []float32) (xids []int64, err error) {
	nq := len(xq) / vm.dim
	dis := make([]float32, nq)
	xids = make([]int64, nq)
	for i := 0; i < nq; i++ {
		dis[i] = vm.distThr
		xids[i] = int64(-1)
	}
	dis2 := make([]float32, nq)
	xids2 := make([]int64, nq)
	var total int
	for _, vdb := range vm.vdbs {
		if total, err = vdb.Search(xq, dis2, xids2); err != nil {
			return
		}
		if total == 0 {
			continue
		}
		for i := 0; i < nq; i++ {
			if xids[i] == int64(-1) || VectodbCompareDistance(vm.metricType, dis2[i], dis[i]) {
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
func (vm *VectodbMulti) AddWithIds(xb []float32, xids []int64) (err error) {
	var quota, total, added int
	nb := len(xids)
	vdb := vm.vdbs[len(vm.vdbs)-1]
	for added < nb {
		if total, err = vdb.GetTotal(); err != nil {
			return
		}
		quota = vm.sizeLimit - total
		if quota > 0 {
			batch := MinInt(quota, nb-added)
			if err = vdb.AddWithIds(xb[added*vm.dim:(added+batch)*vm.dim], xids[added:added+batch]); err != nil {
				return
			}
			added += batch
		} else {
			vm.maxSeq++
			dp := filepath.Join(vm.workDir, getWorkDir(vm.maxSeq))
			if vdb, err = NewVectoDB(dp, vm.dim, vm.metricType, vm.indexKey, vm.queryParams, vm.distThr, vm.sizeLimit/200); err != nil {
				return
			}
			nextXid := vm.maxSeq * vm.sizeLimit
			vdb.SetNextXid(nextXid)
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
func (vm *VectodbMulti) UpdateWithIds(xb []float32, xids []int64) (err error) {
	for _, vdb := range vm.vdbs {
		if err = vdb.UpdateWithIds(xb, xids); err != nil {
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
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker:
				log.Printf("build iteration begin")
				vdbs := vm.vdbs
				for _, vdb := range vdbs {
					if err = vdb.UpdateIndex(); err != nil {
						log.Fatalf("%+v", err)
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
