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

func (ctl *Controller) leaderChangedCb(prevLeader, curLeader string) {
	if ctl.ListenAddr == curLeader && !isLeader {
		log.Infof("I've been promoted as leader")
		ctl.isLeader = true
		ctl.ctxL, ctl.cancelL = context.WithCancel(ctl.ctx)
		go ctl.servBalance(ctl.ctxL)
	} else if ctl.ListenAddr!=curLeader && isLeader {
		log.Infof("I've resigned as follower")
		ctl.isLeader = false
		ctl.cancelL()
	}
}

func (ctl *Controller) servBalance(ctx context.Context) {
	ticker := time.Ticker(60*time.Second)
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

func (ctl *Controller) balance(ctx context.Context) (err error){
}

// @Description Move a vectodblite from this node to the given node
// @Accept  json
// @Produce  json
// @Param   add		body	main.ReqMove	true 	"ReqMove"
// @Success 200 {object} main.RspMove "RspMove"
// @Failure 300 "redirection"
// @Failure 400
// @Router /api/v1/move [post]
func (ctl *Controller) HandleMove(c *gin.Context) {
	var reqMove ReqMove
	var err error
	if err = c.ShouldBind(&reqMove); err != nil {
		err = errors.Wrap(err, "")
		log.Errorf("got error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else {
		var rspMove RspMove
		err = ctl.move(reqMove.DbID, reqMove.DstNode)
		if err != nil {
			rspMove.Err = err.Error()
			log.Errorf("got error %+v", err)
		}
		c.JSON(200, rspAdd)
	}
}

func (ctl *Controller) move(dbID int, dstNode string) (err error) {
	return
}
