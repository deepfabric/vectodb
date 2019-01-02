package main

import (
	"net/http"
	"strconv"

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

type RspSearch struct {
	Xid      uint64  `json:"xid"`
	Distance float32 `json:"distance"`
	Err      string  `json:"err"`
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

// refers to https://github.com/swaggo/swag#api-operation
// github.com/swaggo/swag/operation.go, (*Operation).ParseParamComment, (*Operation).ParseResponseComment
// @Description Add a vector to the given vectodblite
// @Accept  json
// @Produce  json
// @Param   add		body	main.ReqAdd	true 	"ReqAdd. If xid is 0 or ^uint64(0), the cluster will generate one."
// @Success 200 {object} main.RspAdd "RspAdd"
// @Failure 300 "redirection"
// @Failure 400
// @Router /api/v1/add [post]
func (ctl *Controller) HandleAdd(c *gin.Context) {
	var reqAdd ReqAdd
	var err error
	if err = c.ShouldBind(&reqAdd); err != nil {
		err = errors.Wrap(err, "")
		log.Printf("got error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else {
		var rspAdd RspAdd
		if reqAdd.Xid == 0 || reqAdd.Xid == ^uint64(0) {
			rspAdd.Xid, err = ctl.add(reqAdd.DbID, reqAdd.Xb)
		} else {
			rspAdd.Xid = reqAdd.Xid
			err = ctl.addWithId(reqAdd.DbID, reqAdd.Xb, rspAdd.Xid)
		}
		if err != nil {
			rspAdd.Err = err.Error()
			log.Printf("got error %+v", err)
		}
		c.JSON(200, rspAdd)
	}
}

// @Description Search a vector in the given vectodblite
// @Accept  json
// @Produce  json
// @Param   search		body	main.ReqSearch	true 	"ReqSearch"
// @Success 200 {object} main.RspSearch "RspSearch"
// @Failure 300 "redirection"
// @Failure 400
// @Router /api/v1/search [post]
func (ctl *Controller) HandleSearch(c *gin.Context) {
	var reqSearch ReqSearch
	var err error
	if err = c.ShouldBind(&reqSearch); err != nil {
		err = errors.Wrap(err, "")
		log.Printf("got error %+v", err)
		c.String(http.StatusBadRequest, err.Error())
	} else {
		var rspSearch RspSearch
		rspSearch.Xid, rspSearch.Distance, err = ctl.search(reqSearch.DbID, reqSearch.Xq)
		if err != nil {
			rspSearch.Err = err.Error()
			log.Printf("got error %+v", err)
		}
		c.JSON(200, rspSearch)
	}
}

func (ctl *Controller) add(dbID int, xb []float32) (xid uint64, err error) {
	var dbl *vectodb.VectoDBLite
	var ok bool
	if dbl, ok = ctl.dbls[strconv.Itoa(dbID)]; !ok {
		if dbl, err = vectodb.NewVectoDBLite(ctl.conf.RedisAddr, dbID, ctl.conf.Dim, float32(ctl.conf.DisThr), ctl.conf.SizeLimit); err != nil {
			return
		}
	}
	xid, err = dbl.Add(xb)
	return
}

func (ctl *Controller) addWithId(dbID int, xb []float32, xid uint64) (err error) {
	var dbl *vectodb.VectoDBLite
	var ok bool
	if dbl, ok = ctl.dbls[strconv.Itoa(dbID)]; !ok {
		if dbl, err = vectodb.NewVectoDBLite(ctl.conf.RedisAddr, dbID, ctl.conf.Dim, float32(ctl.conf.DisThr), ctl.conf.SizeLimit); err != nil {
			return
		}
	}
	err = dbl.AddWithId(xb, xid)
	return
}

func (ctl *Controller) search(dbID int, xq []float32) (xid uint64, distance float32, err error) {
	var dbl *vectodb.VectoDBLite
	var ok bool
	if dbl, ok = ctl.dbls[strconv.Itoa(dbID)]; !ok {
		if dbl, err = vectodb.NewVectoDBLite(ctl.conf.RedisAddr, dbID, ctl.conf.Dim, float32(ctl.conf.DisThr), ctl.conf.SizeLimit); err != nil {
			return
		}
	}
	xid, distance, err = dbl.Search(xq)
	return
}
