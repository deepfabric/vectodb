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

func (ctl *Controller) holdDb(dbID int) (nodeAddr string, err error) {
	return
}

//TODO: start holdKeepalive goroutine
func (ctl *Controller) holdKeepalive(ctx context.Context) {
	ticker := time.Ticker(30*time.Second)
	var err error
	for {
		select {
		case <-s.Done():
			log.Info("balance goroutine exited due to context done")
			return
		case <- ticker:
			if err = ctl.balance(ctx); err != nil {
				log.Errorf("got error %+v", err)
			}
		}
	}
	return
}
