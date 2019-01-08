package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	ClusterSize      = 5
	ClusterPortBegin = 6731
	Dim              = 128
	SizeLimit        = 10000
	ShopIdBegin      = 1000
	ShopNum          = 100
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
		err = errors.Wrapf(err, "failed to encode reqObj: %+v", reqObj)
		return
	}
	var rsp *http.Response
	if rsp, err = hc.Post(servURL, "application/json", bytes.NewReader(reqBody)); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	rspBody, err = ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if err = json.Unmarshal(rspBody, rspObj); err != nil {
		err = errors.Wrapf(err, "failed to decode rspBody: %+v", string(rspBody))
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
	cmd := []string{"docker-compose", "--file", "docker-compose.yml", "--project-name", "vdblcc_bb", "up", "-d"}
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

func getApiURL(nodeSeq int, method string) (URL string) {
	return fmt.Sprintf("http://127.0.0.1:%d/api/v1/%s", ClusterPortBegin+nodeSeq, method)
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

func main() {
	log.SetLevel(log.DebugLevel)
	var err error
	if err = setupEnv(true); err != nil {
		log.Fatalf("got error %+v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		log.Infof("cancel ctx")
		// Wait subprocesses be killed.
		time.Sleep(5 * time.Second)
		log.Infof("bye")
	}()
	shopDbCache := make(map[int][]Record)
	hc := &http.Client{Timeout: time.Second * 5}

	for i := 0; i < ClusterSize; i++ {
		cmd := []string{"../vectodblite_cluster/vectodblite_cluster",
			"--listen-addr", fmt.Sprintf("127.0.0.1:%d", ClusterPortBegin+i),
			"--dim", strconv.Itoa(Dim),
			"--size-limit", strconv.Itoa(SizeLimit),
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

	// Wait the cluster be up.
	time.Sleep(5 * time.Second)

	for i := 0; i < ShopNum; i++ {
		shopId := ShopIdBegin + i
		records := make([]Record, 0)
		rspAdd := &RspAdd{}
		urlAdd := getApiURL(rand.Intn(ClusterSize), "add")
		for j := 0; j < SizeLimit; j++ {
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

	for i := 0; i < ShopNum; i++ {
		shopId := ShopIdBegin + i
		records := shopDbCache[shopId]
		rspSearch := &RspSearch{}
		urlSearch := getApiURL(rand.Intn(ClusterSize), "search")
		for j := 0; j < SizeLimit; j++ {
			reqSearch := ReqSearch{
				DbID: shopId,
				Xq:   records[j].Vec,
			}
			if err = PostJson(hc, urlSearch, reqSearch, rspSearch); err != nil {
				goto QUIT
			}
			if rspSearch.Err != "" {
				err = errors.New(rspSearch.Err)
				goto QUIT
			}
			if rspSearch.Xid != records[j].Xid {
				err = errors.Errorf("incorrect xid, want %v, have %v", records[j].Xid, rspSearch.Xid)
				goto QUIT
			}
		}
	}
	//TODO: db over-size
	//TODO: load balance
	//TODO: node temporary/permenant failure
QUIT:
	if err != nil {
		log.Errorf("got error %+v", err)
	}
}
