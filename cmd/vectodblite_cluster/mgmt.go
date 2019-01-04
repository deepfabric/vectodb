package main

import (
	"net/http"
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/gin-gonic/gin"
	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)


func (ctl *Controller) nodeKeepalive(ctx context.Context) (err error){
	k := fmt.Sprintf("%s/node/%d", ctl.conf.EurekaApp, ctl.conf.ListenAddr)
	val := "alive"
	txn := ctl.etcdCli.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
	txn = txn.Then(v3.OpPut(k, val, v3.WithLease(concurrency.WithTTL(60))))
	_, err := txn.Commit()
	if err != nil {
		err = errors.Wrap(err, "")
		return err
	}
	return
}

func (ctl *Controller) leaderChangedCb(prevLeader, curLeader string) {
	ctl.curLeader = curLeader
	if ctl.ListenAddr == curLeader && !isLeader {
		log.Infof("I've been promoted as leader")
		ctl.isLeader = true
		ctl.ctxL, ctl.cancelL = context.WithCancel(ctl.ctx)
		go ctl.servLeaderWork(ctl.ctxL)
	} else if ctl.ListenAddr!=curLeader && isLeader {
		log.Infof("I've resigned as follower")
		ctl.isLeader = false
		ctl.cancelL()
	}
}

func (ctl *Controller) servLeaderWork(ctx context.Context) {
	var err error
	load := make(map[string][]int, 0)
	pfx := fmt.Sprintf("%s/vectodblite", ctl.conf.EurekaApp)
	var resp *clientv3.GetResponse
	if resp, err = clientv3.NewKV(ctl.etcdCli).Get(ctx, pfx, clientv3.WithPrefix()); err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+x", err)
	}
	for _, item := range resp.Kvs {
		strDbID := filepath.Base(item.Key)
		var dbID int
		if dbID, err = strconv.Atoi(strDbID); err != nil {
			err = errors.Wrap(err, "")
			log.Fatalf("got error %+x", err)
		}
		nodeAddr := item.Value
		var dbList []int
		if dbList, ok := aliveNodes[nodeAddr], !ok {
			dbList = []int{}
		}
		dbList = append(dbList, dbID)
		load[nodeAddr] = dbList
	}

	aliveNodes := make(map[string]int, 0)
	pfx = fmt.Sprintf("%s/node", ctl.conf.EurekaApp)
	if resp, err = clientv3.NewKV(ctl.etcdCli).Get(ctx, pfx, clientv3.WithPrefix()); err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+x", err)
	}
	for _, item := range resp.Kvs {
		nodeAddr := filepath.Base(item.Key)
		if _, ok := aliveNodes[nodeAddr], !ok {
			alive[nodes] = 0
		}
	}
	revision := resp.Header.Revision

	if err = ctl.purgeDeadNodes(load, aliveNodes); err != nil {
		log.Fatalf("got error %+x", err)
	}

	watcher := clientv3.NewWatcher(ctl.etcdCli)
	nodeChangeCh := watcher.Watch(ctx, pfx, clientv3.WithPrefix(), clientv3.WithRev(revision+1))

	ticker := time.Ticker(60*time.Second)
	var err error
	for {
		select {
		case <-s.Done():
			log.Info("balance goroutine exited due to context done")
			return
		case nc: = <- nodeChangeCh:
			log.Infof("node change: %+v", nc)
		case <- ticker:
			if err = ctl.leaderWork(ctx); err != nil {
				log.Errorf("got error %+v", err)
			}
		}
	}
	return
}


func (ctl *Controller) purgeDeadNode(load map[string][]int, activeNodes map[string]int) (err error){
	for nodeAddr, dbList := range load {
		if _, ok := aliveNodes[nodeAddr], !ok {
			// deleate from etcd
			for _, dbID := range dbList {
				key := fmt.Sprintf("%s/vectodblite/%s", ctl.conf.EurekaApp)
				if _, err = clientv3.NewKV(ctl.etcdCli).Delete(ctx, key); err != nil {
					err = errors.Wrap(err, "")
					log.Errorf("got error %+x", err)
				}

			}
			delete(load, nodeAddr)
		}
	}
}


func (ctl *Controller) leaderWork(ctx context.Context) (err error){
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
		log.Errorf("got error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else if !ctl.isLeader && ctl.curLeader != "" {
		dstURL := *c.Request.URL
		dstURL.Host = ctl.curLeader
		c.Redirect(http.StatusMovedPermanently, dstURL.String())
	} else {
		rspAcquire := RspAcquire {
			DbID: reqAcquire.DbID,
		}
		rspAcquire.NodeAddr, err = ctl.acquire(reqAcquire.DbID, reqAcquire.NodeAddr)
		if err != nil {
			rspAcquire.Err = err.Error()
			log.Errorf("got error %+v", err)
		}
		c.JSON(200, rspAcuqire)
	}
}

func (ctl *Controller) acquire(dbID int, nodeAddr string) (dstNodeAddr string, err error) {
	if !ctl.isLeader {
		err = errors.Errorf("not capable to acquire since I'm not the leader")
		return
	}
	k := fmt.Sprintf("%s/vectodblite/%d", ctl.conf.EurekaApp, dbID)
	val := nodeAddr
	txn := ctl.etcdCli.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
	txn = txn.Then(v3.OpPut(k, val))
	txn = txn.Else(v3.OpGet(k))
	resp, err := txn.Commit()
	if err != nil {
		err = errors.Wrap(err, "")
		return err
	}
	kv := resp.Responses[0].GetResponseRange().Kvs[0]
	dstNodeAddr = string(kv.Value)
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
		log.Errorf("got error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else {
		ctl.rwlock.Lock()
		defer ctl.rwlock.Unlock()
		rspRelease := RspRelease{
			DbID: reqAcquire.DbID
		}
		dbID := reqAcquire.DbID
		var dbl *vectodb.VectoDBLite
		ctl.rwlock.Lock()
		defer ctl.rwlock.Unlock()
		if dbl, ok = ctl.dbls[dbID]; ok {
			delete(ctl.dbls, dbID)
			if err = dbl.Destroy(); err != nil {
				log.Errorf("got error %+v", err)
				rspRelease.Err = err.Error()
			} else{
				log.Infof("released vectodblite %d", dbID)
			}
		} else {
			log.Infof("skipped releasing vectodblite %d", dbID)
		}
		c.JSON(200, rspRelease)
	}
}
