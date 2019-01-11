package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"

	"github.com/hudl/fargo"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	ClusterSize              = 5
	ClusterPortBegin         = 6731
	Dim                      = 128
	DisThr           float32 = 0.9
	SizeLimit                = 100
	SizeExtra                = 5
	ShopIdBegin              = 1000
	ShopNum                  = 100
	BalanceInterval          = 20
)

var (
	//EurekaAddr = "http://127.0.0.1:8080/eureka/v2"
	EurekaAddr = "http://127.0.0.1:8761/eureka"
	//EurekaAddr = "http://172.19.0.101:8761/eureka"
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

type Record struct {
	Vec []float32
	Xid uint64
}

type ReqCommon struct {
	DbID int `json:"dbID"`
}

type Router struct {
	rwlock      sync.RWMutex
	nodeAddrs   []string       // All nodes' address. It shall not be empty.
	routeTable  map[int]string //dbID -> nodeAddr. It's empty at begining, and be updated whenever a redirect occurs.
	numRandom   int
	numRedirect int
}

func NewRouter(nodeAddrs []string) (rt *Router) {
	rt = &Router{
		nodeAddrs:  nodeAddrs,
		routeTable: make(map[int]string, 0),
	}
	return
}

func (rt *Router) SetNodeAddrs(nodeAddrs []string) {
	rt.rwlock.Lock()
	defer rt.rwlock.Unlock()
	rt.nodeAddrs = nodeAddrs
	for shopID := range rt.routeTable {
		delete(rt.routeTable, shopID)
	}
}

func (rt *Router) GetRoute(dbID int) (nodeAddr string) {
	rt.rwlock.RLock()
	defer rt.rwlock.RUnlock()
	var ok bool
	if nodeAddr, ok = rt.routeTable[dbID]; ok {
		return
	}
	idx := rand.Intn(len(rt.nodeAddrs))
	nodeAddr = rt.nodeAddrs[idx]
	rt.routeTable[dbID] = nodeAddr
	rt.numRandom++
	return
}

func (rt *Router) Print() (sdev float64) {
	rt.rwlock.RLock()
	defer rt.rwlock.RUnlock()
	reverseTable := make(map[string][]int, 0)
	for dbID, nodeAddr := range rt.routeTable {
		var dbList []int
		var ok bool
		if dbList, ok = reverseTable[nodeAddr]; !ok {
			dbList = []int{dbID}
		} else {
			dbList = append(dbList, dbID)
		}
		reverseTable[nodeAddr] = dbList
	}
	var msg string
	nodeAddrs := make([]string, 0)
	for nodeAddr := range reverseTable {
		nodeAddrs = append(nodeAddrs, nodeAddr)
	}
	sort.Strings(nodeAddrs)
	lens := make([]float64, 0)
	for _, nodeAddr := range nodeAddrs {
		dbList, _ := reverseTable[nodeAddr]
		sort.Ints(dbList)
		lens = append(lens, float64(len(dbList)))
		msg += fmt.Sprintf("%s(%d): %+v\n", nodeAddr, len(dbList), dbList)
	}
	sdev, _ = stats.Variance(lens)
	log.Infof("router numRandom %d, numRedirect %d, route table variance %v, route table:\n%s", rt.numRandom, rt.numRedirect, sdev, msg)
	return
}

func (rt *Router) CheckRedirect(req *http.Request, via []*http.Request) error {
	reqBody, err := ioutil.ReadAll(req.Body)
	// https://stackoverflow.com/questions/23070876/reading-body-of-http-request-without-modifying-request-state
	req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBody))
	reqCommon := &ReqCommon{}
	if err = json.Unmarshal(reqBody, &reqCommon); err != nil {
		log.Errorf("got error %+v", err)
		return nil
	}
	nodeAddr := req.Host
	if nodeAddr == "" {
		nodeAddr = req.URL.Host
	}
	log.Debugf("DbID %v, req.Host %v, req.URL %+v, nodeAddr %v", reqCommon.DbID, req.Host, req.URL, nodeAddr)
	rt.rwlock.Lock()
	rt.routeTable[reqCommon.DbID] = nodeAddr
	rt.rwlock.Unlock()
	rt.numRedirect++
	return nil
}

func runCmd(cmd []string) (err error) {
	log.Debugf(strings.Join(cmd, " "))
	var output []byte
	if output, err = exec.Command(cmd[0], cmd[1:]...).Output(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	sOutput := string(output)
	log.Debugf(sOutput)
	return
}

func startCmd(ctx context.Context, cmd []string, stdout, stderr io.Writer) (err error) {
	log.Debugf(strings.Join(cmd, " "))
	command := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	command.Stdout = stdout
	command.Stderr = stderr
	if err = command.Start(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func PostJson(hc *http.Client, servURL string, reqObj, rspObj interface{}) (err error) {
	var reqBody, rspBody []byte
	if reqBody, err = json.Marshal(reqObj); err != nil {
		err = errors.Wrapf(err, "servURL %+v, failed to encode reqObj: %+v", servURL, reqObj)
		return
	}
	var rsp *http.Response
	if rsp, err = hc.Post(servURL, "application/json", bytes.NewReader(reqBody)); err != nil {
		err = errors.Wrapf(err, "servURL %+v", servURL)
		return
	}
	rspBody, err = ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()
	if err != nil {
		err = errors.Wrapf(err, "servURL %+v", servURL)
		return
	}
	if err = json.Unmarshal(rspBody, rspObj); err != nil {
		err = errors.Wrapf(err, "servURL %+v, failed to decode rspBody: %+v", servURL, string(rspBody))
		return
	}
	return
}

func setupEnv(clear bool) (err error) {
	if clear {
		if err = teardownEnv(true); err != nil {
			return
		}
	}
	cmd := []string{"docker-compose", "--file", "docker-compose.yml", "up", "-d"}
	err = runCmd(cmd)
	return
}

func teardownEnv(clear bool) (err error) {
	var cmd []string
	if clear {
		cmd = []string{"docker-compose", "--file", "docker-compose.yml", "down"}

	} else {
		cmd = []string{"docker-compose", "--file", "docker-compose.yml", "stop"}
	}
	err = runCmd(cmd)
	return
}

func getApiURL(nodeAddr, method string) (URL string) {
	return fmt.Sprintf("http://%s/api/v1/%s", nodeAddr, method)
}

func genVec() (vec []float32) {
	vec = make([]float32, Dim)
	var prod float64
	for i := 0; i < Dim; i++ {
		vec[i] = rand.Float32()
		prod += float64(vec[i]) * float64(vec[i])
	}
	prod = math.Sqrt(prod)
	for i := 0; i < Dim; i++ {
		vec[i] = float32(float64(vec[i]) / prod)
	}
	return
}

func search(shopDbCache map[int][]Record, hc *http.Client, router *Router) (err error) {
	for i := 0; i < ShopNum; i++ {
		shopId := ShopIdBegin + i
		records := shopDbCache[shopId]
		rspSearch := &RspSearch{}
		log.Infof("searching vectors in shop %d...", shopId)
		for j := 0; j < SizeLimit+SizeExtra; j++ {
			nodeAddr := router.GetRoute(shopId)
			urlSearch := getApiURL(nodeAddr, "search")
			reqSearch := ReqSearch{
				DbID: shopId,
				Xq:   records[j].Vec,
			}
			if err = PostJson(hc, urlSearch, reqSearch, rspSearch); err != nil {
				return
			}
			if rspSearch.Err != "" {
				err = errors.New(rspSearch.Err)
				return
			}
			if rspSearch.Xid != ^uint64(0) && rspSearch.Distance < DisThr {
				err = errors.Errorf("incorrect distance for vector %d, want >=%v, have %v.", j, DisThr, rspSearch.Distance)
				return
			}
			if j < SizeExtra {
				if rspSearch.Xid != ^uint64(0) {
					err = errors.Errorf("incorrect xid for vector %d, want %016x, have %016x. distance %v.", j, ^uint64(0), rspSearch.Xid, rspSearch.Distance)
					return
				}
			} else {
				if rspSearch.Xid != records[j].Xid {
					err = errors.Errorf("incorrect xid for vector %d, want %016x, have %016x. distance %v.", j, records[j].Xid, rspSearch.Xid, rspSearch.Distance)
					return
				}
			}
		}
	}
	return
}

// get node addrs from eureka
func getNodeAddrs() (nodeAddrs []string, err error) {
	conn := fargo.NewConn(EurekaAddr)
	var app *fargo.Application
	if app, err = conn.GetApp("vectodblite-cluster"); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	nodeAddrs = make([]string, len(app.Instances))
	for i, ins := range app.Instances {
		nodeAddr := fmt.Sprintf("%s:%d", ins.IPAddr, ins.Port)
		nodeAddrs[i] = nodeAddr
		/*
			// It's likely that Eureka has not purge dead nodes for some reason.
			lastRenewal := ins.LeaseInfo.LastRenewalTimestamp / 1000
			now := time.Now().Unix()
			durationInSeconds := ins.LeaseInfo.DurationInSecs
			if lastRenewal+int64(durationInSeconds*3) > now {
				nodeAddr := fmt.Sprintf("%s:%d", ins.IPAddr, ins.Port)
				nodeAddrs[i] = nodeAddr
			} else {
				log.Infof("found dead node %s:%d, lastRenewal %d, now %d", ins.IPAddr, ins.Port, lastRenewal, now)
			}
		*/
	}
	sort.Strings(nodeAddrs)
	return
}

func main() {
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	log.SetFormatter(formatter)
	log.SetLevel(log.DebugLevel)
	var err error
	clear := true
	if err = setupEnv(clear); err != nil {
		log.Fatalf("got error %+v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		log.Infof("cancel ctx")
		// Wait the cluster subprocesses be killed.
		time.Sleep(5 * time.Second)
		log.Infof("bye")
	}()
	shopDbCache := make(map[int][]Record)
	hc := &http.Client{Timeout: time.Second * 5}
	nodeAddrs := make([]string, 0)
	var router *Router
	var sdev0, sdev1 float64
	var nodeAddrs2 []string
	var eurekaOk bool

	// Start the cluster.
	for i := 0; i < ClusterSize; i++ {
		nodeAddrs = append(nodeAddrs, fmt.Sprintf("127.0.0.1:%d", ClusterPortBegin+i))
		cmd := []string{"../vectodblite_cluster/vectodblite_cluster",
			"--listen-addr", fmt.Sprintf("127.0.0.1:%d", ClusterPortBegin+i),
			"--dim", strconv.Itoa(Dim),
			"--distance-threshold", fmt.Sprintf("%v", DisThr),
			"--size-limit", strconv.Itoa(SizeLimit),
			"--balance-interval", strconv.Itoa(BalanceInterval),
			"--eureka-addr", EurekaAddr,
			"--debug", "true",
		}
		var f *os.File
		if f, err = os.OpenFile(fmt.Sprintf("%d.log", ClusterPortBegin+i), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			err = errors.Wrap(err, "")
			goto QUIT
		}
		if err = startCmd(ctx, cmd, f, f); err != nil {
			goto QUIT
		}
	}

	// Wait the cluster be ready: an instance has been elected as leader, and all have registered with Eureka.
	// Eureka bootstrap and instance registration is slow.
	for i := 0; i < 4; i++ {
		time.Sleep(20 * time.Second)
		if nodeAddrs2, err = getNodeAddrs(); err != nil {
			goto QUIT
		}
		log.Infof("Instances in eureka: %+v", nodeAddrs2)
		if ok := reflect.DeepEqual(nodeAddrs, nodeAddrs2); ok {
			eurekaOk = true
			break
		}
	}
	if !eurekaOk {
		err = errors.Errorf("Instances in eureka doesn't match expectation, want %v, have %v", nodeAddrs, nodeAddrs2)
		goto QUIT
	}

	// Fill all databases with random vectors
	log.Infof("adding vectors...")
	router = NewRouter(nodeAddrs)
	hc.CheckRedirect = router.CheckRedirect
	for i := 0; i < ShopNum; i++ {
		shopId := ShopIdBegin + i
		records := make([]Record, 0)
		rspAdd := &RspAdd{}
		for j := 0; j < SizeLimit+SizeExtra; j++ {
			nodeAddr := router.GetRoute(shopId)
			urlAdd := getApiURL(nodeAddr, "add")
			reqAdd := ReqAdd{
				DbID: shopId,
				Xb:   genVec(),
			}
			if err = PostJson(hc, urlAdd, reqAdd, rspAdd); err != nil {
				goto QUIT
			}
			if rspAdd.Err != "" {
				err = errors.New(rspAdd.Err)
				goto QUIT
			}
			records = append(records, Record{
				Vec: reqAdd.Xb,
				Xid: rspAdd.Xid,
			})
		}
		shopDbCache[shopId] = records
	}
	sdev0 = router.Print()

	// Search the vector just inserted, expect to get the same xid as insertion for the last SizeLimit vectors.
	if err = search(shopDbCache, hc, router); err != nil {
		goto QUIT
	}

	if err = search(shopDbCache, hc, router); err != nil {
		goto QUIT
	}

	// Wait the cluster be balanced.
	time.Sleep(time.Duration(1.5*BalanceInterval) * time.Second)
	// Search again, expect to get balanced route.
	router = NewRouter(nodeAddrs)
	hc.CheckRedirect = router.CheckRedirect
	if err = search(shopDbCache, hc, router); err != nil {
		goto QUIT
	}
	sdev1 = router.Print()
	if sdev1 > sdev0 {
		err = errors.Errorf("route table variance doesn't match expectation, want <=%v, have %v", sdev0, sdev1)
		goto QUIT
	}

	// Check registration in Eureka again
	if nodeAddrs2, err = getNodeAddrs(); err != nil {
		goto QUIT
	}
	if ok := reflect.DeepEqual(nodeAddrs, nodeAddrs2); !ok {
		err = errors.Errorf("Instances' address are incorrect, want %v, have %v", nodeAddrs, nodeAddrs2)
		goto QUIT
	}

	//TODO: node temporary/permenant failure
QUIT:
	if err != nil {
		log.Errorf("got error %+v", err)
	}
}
