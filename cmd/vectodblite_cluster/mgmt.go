package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	MaxLoadDelta = 2
)

func (ctl *Controller) nodeKeepalive(ctx context.Context) (err error) {
	resp, err := ctl.etcdCli.Grant(ctx, int64(60))
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	// the key will be kept forever
	_, kaerr := ctl.etcdCli.KeepAlive(ctx, resp.ID)
	if kaerr != nil {
		err = errors.Wrap(err, "")
		return
	}
	leaseID := resp.ID

	k := fmt.Sprintf("%s/node/%s", ctl.conf.EurekaApp, ctl.conf.ListenAddr)
	val := "alive"
	txn := ctl.etcdCli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0))
	txn = txn.Then(clientv3.OpPut(k, val, clientv3.WithLease(leaseID)))
	if _, err = txn.Commit(); err != nil {
		err = errors.Wrap(err, "")
		return err
	}
	return
}

func (ctl *Controller) leaderChangedCb(prevLeader, curLeader string) {
	ctl.curLeader = curLeader
	if ctl.conf.ListenAddr == curLeader && !ctl.isLeader {
		log.Infof("I've been promoted as leader")
		ctl.isLeader = true
		ctl.ctxL, ctl.cancelL = context.WithCancel(ctl.ctx)
		go ctl.servLeaderWork(ctl.ctxL)
	} else if ctl.conf.ListenAddr != curLeader && ctl.isLeader {
		log.Infof("I've resigned as follower")
		ctl.isLeader = false
		ctl.cancelL()
	}
}

func (ctl *Controller) servLeaderWork(ctx context.Context) {
	var err error
	aliveNodes := make(map[string]int, 0)
	pfx := fmt.Sprintf("%s/node", ctl.conf.EurekaApp)
	var resp *clientv3.GetResponse
	if resp, err = clientv3.NewKV(ctl.etcdCli).Get(ctx, pfx, clientv3.WithPrefix()); err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+x", err)
	}
	for _, item := range resp.Kvs {
		nodeAddr := filepath.Base(string(item.Key))
		aliveNodes[nodeAddr] = 0
	}
	revision := resp.Header.Revision

	var load map[string][]int
	if load, err = ctl.getLoad(); err != nil {
		log.Fatalf("got error %+x", err)
	}
	if err = ctl.purgeDeadNodes(load, aliveNodes); err != nil {
		log.Fatalf("got error %+x", err)
	}

	watcher := clientv3.NewWatcher(ctl.etcdCli)
	nodeChangeCh := watcher.Watch(ctx, pfx, clientv3.WithPrefix(), clientv3.WithRev(revision+1))

	balanceInterval := time.Duration(ctl.conf.BalanceInterval) * time.Second
	balanceTick := time.After(balanceInterval)
	log.Debugf("balance interval is %v", balanceInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info("balance goroutine exited due to context done")
			return
		case nc := <-nodeChangeCh:
			log.Debugf("node change: %+v", nc)
			if err = nc.Err(); err != nil {
				err = errors.Wrap(err, "")
				log.Errorf("got error %+x", err)
			}
			for _, e := range nc.Events {
				nodeAddr := filepath.Base(string(e.Kv.Key))
				if e.Type == clientv3.EventTypeDelete {
					delete(aliveNodes, nodeAddr)
				} else if e.IsCreate() || e.IsModify() {
					aliveNodes[nodeAddr] = 0
				}
			}
			if err = ctl.purgeDeadNodes(load, aliveNodes); err != nil {
				log.Fatalf("got error %+x", err)
			}
		case <-balanceTick:
			if load, err = ctl.getLoad(); err != nil {
				log.Errorf("got error %+x", err)
			}
			if err = ctl.balance(load); err != nil {
				log.Errorf("got error %+v", err)
			}
			balanceTick = time.After(balanceInterval)
		}
	}
}

func (ctl *Controller) getLoad() (load map[string][]int, err error) {
	load = make(map[string][]int, 0)
	pfx := fmt.Sprintf("%s/vectodblite", ctl.conf.EurekaApp)
	var resp *clientv3.GetResponse
	if resp, err = clientv3.NewKV(ctl.etcdCli).Get(ctl.ctx, pfx, clientv3.WithPrefix()); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	for _, item := range resp.Kvs {
		strDbID := filepath.Base(string(item.Key))
		var dbID int
		if dbID, err = strconv.Atoi(strDbID); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		nodeAddr := string(item.Value)
		var dbList []int
		var ok bool
		if dbList, ok = load[nodeAddr]; !ok {
			dbList = []int{}
		}
		dbList = append(dbList, dbID)
		load[nodeAddr] = dbList
	}
	log.Infof("cluster load %+v", load)
	return
}

func (ctl *Controller) purgeDeadNodes(load map[string][]int, aliveNodes map[string]int) (err error) {
	for nodeAddr, dbList := range load {
		if _, ok := aliveNodes[nodeAddr]; !ok {
			for _, dbID := range dbList {
				key := fmt.Sprintf("%s/vectodblite/%d", ctl.conf.EurekaApp, dbID)
				if _, err = clientv3.NewKV(ctl.etcdCli).Delete(ctl.ctxL, key); err != nil {
					err = errors.Wrap(err, "")
					return
				}
			}
			delete(load, nodeAddr)
			log.Infof("purged dead node %v", nodeAddr)
		}
	}
	return
}

func (ctl *Controller) balance(load map[string][]int) (err error) {
	if len(load) < 2 {
		log.Infof("skipped balancing since number of nodes %d is less than 2", len(load))
		return
	}
	var totalDbLen int
	for _, dbList := range load {
		totalDbLen += len(dbList)
	}
	avgDbLen := totalDbLen / len(load)

	for nodeAddr, dbList := range load {
		dbLen := len(dbList)
		if dbLen-avgDbLen <= MaxLoadDelta {
			continue
		}
		numBalance := dbLen - avgDbLen - MaxLoadDelta
		log.Infof("balancing %d databases from %v", numBalance, nodeAddr)

		for i := 0; i < numBalance; i++ {
			// Pick a random db from the node, tell the node to release it, remove it from etcd and load.
			dbIDIdx := rand.Intn(len(dbList))
			dbID := dbList[dbIDIdx]
			if nodeAddr == ctl.conf.ListenAddr {
				if err = ctl.release(dbID); err != nil {
					return
				}
			} else {
				reqRelease := ReqRelease{
					DbID: dbID,
				}
				rspRelease := &RspRelease{}
				if err = PostJson(ctl.hc, fmt.Sprintf("http://%s/mgmt/v1/release", nodeAddr), reqRelease, rspRelease); err != nil {
					return
				} else if rspRelease.Err != "" {
					err = errors.New(rspRelease.Err)
					return
				}
				key := fmt.Sprintf("%s/vectodblite/%d", ctl.conf.EurekaApp, dbID)
				if _, err = clientv3.NewKV(ctl.etcdCli).Delete(ctl.ctxL, key); err != nil {
					err = errors.Wrap(err, "")
					return
				}
			}
			dbList = append(dbList[:dbIDIdx], dbList[dbIDIdx+1:]...)
		}
		load[nodeAddr] = dbList
	}

	log.Debugf("balancing done. previous avgDbLen %v.", avgDbLen)
	return
}

// @Description Assocaite a vectodblite with the given node. Only the leader node supports this API.
// @Accept  json
// @Produce  json
// @Param   add		body	main.ReqAcquire	true 	"ReqAcquire"
// @Success 200 {object} main.RspAcquire "RspAcquire"
// @Failure 301 "redirection"
// @Failure 400
// @Router /mgmt/v1/acquire [post]
func (ctl *Controller) HandleAcquire(c *gin.Context) {
	var reqAcquire ReqAcquire
	var err error
	if err = c.ShouldBind(&reqAcquire); err != nil {
		err = errors.Wrap(err, "")
		log.Infof("failed to parse request body, error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else if !ctl.isLeader && ctl.curLeader != "" {
		dstURL := *c.Request.URL
		dstURL.Host = ctl.curLeader
		c.Redirect(http.StatusPermanentRedirect, dstURL.String())
	} else {
		rspAcquire := RspAcquire{
			DbID: reqAcquire.DbID,
		}
		ctx := c.Request.Context()
		rspAcquire.NodeAddr, err = ctl.acquire(ctx, reqAcquire.DbID, reqAcquire.NodeAddr)
		if err != nil {
			rspAcquire.Err = err.Error()
			log.Errorf("got error %+v", err)
		}
		c.JSON(200, rspAcquire)
	}
}

func (ctl *Controller) acquire(ctx context.Context, dbID int, nodeAddr string) (dstNodeAddr string, err error) {
	if !ctl.isLeader {
		err = errors.Errorf("not capable to acquire since I'm not the leader")
		return
	}
	k := fmt.Sprintf("%s/vectodblite/%d", ctl.conf.EurekaApp, dbID)
	// https://coreos.com/etcd/docs/latest/learning/api.html
	val := nodeAddr
	txn := ctl.etcdCli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0))
	txn = txn.Then(clientv3.OpPut(k, val))
	txn = txn.Else(clientv3.OpGet(k))
	resp, err := txn.Commit()
	if err != nil {
		err = errors.Wrap(err, "")
		return "", err
	}
	if resp.Succeeded {
		dstNodeAddr = nodeAddr
		log.Infof("acquired vectodblite %d for %s", dbID, nodeAddr)
	} else {
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		dstNodeAddr = string(kv.Value)
		log.Infof("failed to acquire vectodblite %d for %s, it's already acquired by %s", dbID, nodeAddr, dstNodeAddr)
	}
	return
}

// @Description De-associate a vectodblite with this node.
// @Accept  json
// @Produce  json
// @Param   add		body	main.ReqRelease	true 	"ReqRelease"
// @Success 200 {object} main.RspRelease "RspRelease"
// @Failure 301 "redirection"
// @Failure 400
// @Router /mgmt/v1/release [post]
func (ctl *Controller) HandleRelease(c *gin.Context) {
	var reqRelease ReqRelease
	var err error
	if err = c.ShouldBind(&reqRelease); err != nil {
		err = errors.Wrap(err, "")
		log.Infof("failed to parse request body, error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else {
		rspRelease := RspRelease{
			DbID: reqRelease.DbID,
		}
		dbID := reqRelease.DbID
		if err = ctl.release(dbID); err != nil {
			log.Errorf("got error %+v", err)
			rspRelease.Err = err.Error()
		}
		c.JSON(200, rspRelease)
	}
}

func (ctl *Controller) release(dbID int) (err error) {
	ctl.rwlock.Lock()
	defer ctl.rwlock.Unlock()
	if dbl, ok := ctl.dbls[dbID]; ok {
		delete(ctl.dbls, dbID)
		if err = dbl.Destroy(); err != nil {
			return
		} else {
			log.Infof("released vectodblite %d", dbID)
		}
	} else {
		log.Infof("vectodblite %d is already released", dbID)
	}
	return
}
