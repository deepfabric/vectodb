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

func (ctl *Controller) holdDb(ctx context.Context, dbID int) (nodeAddr string, err error) {
	k := fmt.Sprintf("%s/vectodblite/%d", ctl.conf.EurekaApp, dbID)
	val := ctl.conf.ListenAddr
	txn := ctl.etcdCli.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
	txn = txn.Then(v3.OpPut(k, val, v3.WithLease(concurrency.WithTTL(60))))
	txn = txn.Else(v3.OpGet(k))
	resp, err := txn.Commit()
	if err != nil {
		err = errors.Wrap(err, "")
		return err
	}
	kv := resp.Responses[0].GetResponseRange().Kvs[0]
	nodeAddr = string(kv.Value)
	return
}

func (ctl *Controller) holdKeepalive(ctx context.Context) (err error) {
	txn := ctl.etcdCli.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
	for dbID := range ctl.dbls {
		k := fmt.Sprintf("%s/vectodblite/%d", ctl.conf.EurekaApp, dbID)
		val := ctl.conf.ListenAddr
		txn = txn.Then(v3.OpPut(k, val, v3.WithLease(concurrency.WithTTL(60))))
	}
	_, err = txn.Commit()
	if err != nil {
		err = errors.Wrap(err, "")
		return err
	}
	return
}

func (ctl *Controller) servHoldKeepalive(ctx context.Context) {
	tresp, terr := lkv2.Txn(context.TODO()).Then(
		clientv3.OpTxn(nil, opArray, nil),
		clientv3.OpPut("k", "def"),
		clientv3.OpPut("k3", "999"), // + a key not in any cache
	).Commit()
	if terr != nil {
		t.Fatal(terr)
	}
	ticker := time.Ticker(30*time.Second)
	var err error
	for {
		select {
		case <-s.Done():
			log.Info("servHoldKeepalive goroutine exited due to context done")
			return
		case <- ticker:
			if err = ctl.holdKeepalive(ctx); err != nil {
				log.Errorf("got error %+v", err)
			}
		}
	}
	return
}
