package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/infinivision/vectodb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type ReqAdd struct {
	DbID int       `json:"dbID"`
	Xb   []float32 `json:"xb"`
	Xid  uint64    `json:"xid"`
}

type RspAdd struct {
	Xid uint64 `json:"xid"`
	Err string `json:"err"`
}

type ReqSearch struct {
	DbID int       `json:"dbID"`
	Xq   []float32 `json:"xq"`
}

type ControllerConf struct {
	RedisAddr string
	Dim       int
	DisThr    float64
	SizeLimit int

	EurekaAddr string
	EurekaApp  string
}

type Controller struct {
	conf *ControllerConf
	dbls map[string]*vectodb.VectoDBLite
}

func NewControllerConf() (conf *ControllerConf) {
	return &ControllerConf{
		RedisAddr:  "127.0.0.1:6379",
		Dim:        512,
		DisThr:     0.9,
		SizeLimit:  10000,
		EurekaAddr: "http://127.0.0.1:8761/eureka",
		EurekaApp:  "vectodblite-cluster",
	}
}

func NewController(conf *ControllerConf) (ctl *Controller) {
	return &Controller{
		conf: conf,
		dbls: make(map[string]*vectodb.VectoDBLite),
	}
}

// refers to https://github.com/swaggo/swag#api-operation and github.com/swaggo/swag/operation.go
// @Description Add a vector to the given vectodblite
// @Accept  json
// @Produce  json
// @Param   add		body	main.ReqAdd	true 	"ReqAdd. If xid is not specified, the cluster will generate one."
// @Success 200 {object} main.RspAdd "RspAdd"
// @Failure 300 "redirection"
// @Failure 400
// @Router /api/v1/add [post]
func (ctl *Controller) HandleAdd(c *gin.Context) {
	var reqAdd ReqAdd
	if err := c.ShouldBind(&reqAdd); err != nil {
		err = errors.Wrap(err, "")
		log.Printf("got error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else {
		log.Printf("HandleAdd %+v", reqAdd)
		c.String(200, "Success")
	}

}

func (ctl *Controller) HandleSearch(c *gin.Context) {
}

func (ctl *Controller) add(dbID int, xb []float32) (xid uint64, err error) {
	return
}

func (ctl *Controller) addWithId(dbID int, xb []float32, xid uint64) (err error) {
	return
}

func (ctl *Controller) search(dbID, xq []float32) (xid uint64, distance float32, err error) {
	return
}
