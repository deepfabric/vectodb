package main

/*
based on "ectdctl elect" impl code github.com/coreos/etcd/etcdctl/ctlv3/command/elect_command.go
I want to cover following cases in unit test:
etcd up, c1 up (expect it's elected as leader), c2 up(expect the leader is c1), c1 down (at c2, expect leader change to c2)
etcd up, c1 up, c2 up, etcd down, c1 down, etcd up (at c2, expect leader changes to c2)
etcd up, c1 up, etcd down (expect c1 retry connecting in endless loop), etcd up, c2 up (expect c1&c2 agree to the same leader in 60s)
c1 up (expect c1 retry connecting in endless loop), etcd up (at c1, expect it's elected as leader)
*/

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	log "github.com/golang/glog"
	"golang.org/x/net/context"
)

type LeaderChangedHandler func(prevProposal, curProposal string)

type urlParams struct {
	hosts    []string
	path     string
	userName string
	password string
}

func parseResp(resp *clientv3.GetResponse) (k string, v string) {
	for _, kv := range resp.Kvs {
		k, v = string(kv.Key), string(kv.Value)
		return k, v
	}
	return
}

func observe(ctx context.Context, c *clientv3.Client, pfx string, cb LeaderChangedHandler) {
	s, err := concurrency.NewSession(c)
	if err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+v", err)
	}
	e := concurrency.NewElection(s, pfx)

	prevProposal := ""
	for resp := range e.Observe(ctx) {
		//Kvs could be empty(etcd down, leader down, etcd up):
		//&{Header:cluster_id:3373127551666285087 member_id:13506963981489885289 revision:23 raft_term:4  Kvs:[] More:false Count:0}
		k, v := parseResp(&resp)
		if k != "" {
			log.Info(fmt.Sprintf("observed leader: %s %s", k, v))
			if v != prevProposal && cb != nil {
				cb(prevProposal, v)
			}
			prevProposal = v
		} else {
			log.Infof("Observe got empty response")
		}
	}
	log.Info("observe goroutine exited due to context done")
	return
}

func campaign(ctx context.Context, c *clientv3.Client, pfx string, prop string) {
	/**
	According to https://github.com/coreos/etcd/blob/master/etcdctl/README.md,
	The lease length of a leader defaults to 60 seconds. If a candidate is abnormally terminated, election progress may be delayed by up to 60 seconds.
	However I haven't notice that long delay.
	*/
	s, err := concurrency.NewSession(c, concurrency.WithTTL(10))
	if err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("got error %+v", err)
	}
	e := concurrency.NewElection(s, pfx)

	log.Infof("my proposal: %v", prop)
	//Campaign puts a value as eligible for the election. It blocks until it is elected, an error occurs, or the context is cancelled.
	if err = e.Campaign(ctx, prop); err != nil {
		err = errors.Wrap(err, "")
		return
	}

	// print key since elected
	resp, err := c.Get(ctx, e.Key())
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	k, v := parseResp(resp)
	if k != "" {
		log.Info(fmt.Sprintf("I'v been elected as leader: %s %s", k, v))
	} else {
		err = errors.Errorf("Campaign got empty response")
		return
	}
	return
}

func NewEtcdClient(etcdAddr string) (*clientv3.Client, error) {
	//grpc dialing occurs when constructing clientv3.Config.
	//Note that DialTimeout only applis to the first time connecting.
	//2016/08/31 11:42:32 Failed to dial 127.0.0.1:2379: context canceled; please retry.
	//grpc: timed out when dialing
	cfg := &clientv3.Config{
		Endpoints:   strings.Split(etcdAddr, ","),
		DialTimeout: 3 * time.Second,
	}
	client, err := clientv3.New(*cfg)
	if err != nil {
		err = errors.Wrap(err, "")
		log.Error(err)
		return nil, err
	}
	return client, nil
}

//https://blog.golang.org/context, Go Concurrency Patterns: Context
//https://golang.org/pkg/context/
func StartElection(ctx context.Context, client *clientv3.Client, path string, proposal string, cb LeaderChangedHandler) {
	//Note: puting election and jobs at the same path level doesn't work!
	pfx := fmt.Sprintf("%s/election", path)
	go observe(ctx, client, pfx, cb)
	go campaign(ctx, client, pfx, proposal)
}
