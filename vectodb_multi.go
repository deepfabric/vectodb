package vectodb

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

func NewVectodbMulti(workDir string, dim int, sizeLimit int) (vm *VectodbMulti, err error) {
	vm = &VectodbMulti{
		dim:         dim,
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
		vdb, err = NewVectoDB(dp, dim)
		vm.vdbs = append(vm.vdbs, vdb)
	}
	vm.maxSeq = seqs[len(seqs)-1]
	return
}

//Search perform batch search
/**
 * nq       number of query points, shall be equal to len(xids)
 * k        kNN
 * xq       query points
 */
func (vm *VectodbMulti) Search(nq, k int, xq []float32) (res [][]XidScore, err error) {
	res = make([][]XidScore, nq)
	var res2 [][]XidScore
	for _, vdb := range vm.vdbs {
		if res2, err = vdb.Search(k, xq, nil); err != nil {
			return
		}
		for i := 0; i < nq; i++ {
			res[i] = append(res[i], res2[i]...)
		}
	}
	for i := 0; i < nq; i++ {
		sort.Slice(res[i], func(i1, i2 int) bool { return res[i][i1].Score > res[i][i2].Score })
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
			if vdb, err = NewVectoDB(dp, vm.dim); err != nil {
				return
			}
			vm.vdbs = append(vm.vdbs, vdb)
		}

	}
	return
}

//StartBuilderLoop starts a goroutine to build index in loop
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
				log.Infof("build iteration begin")
				vdbs := vm.vdbs
				for _, vdb := range vdbs {
					if err = vdb.SyncIndex(); err != nil {
						log.Fatalf("%+v", err)
					}
					// sleep a while to avoid busy loop
					select {
					case <-ctx.Done():
						return
					case <-ticker:
					}
				}
				log.Infof("build iteration done")
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
