package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

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
