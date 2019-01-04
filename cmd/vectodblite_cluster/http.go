package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

func PostJson(hc *http.Client, servURL string, reqObj, rspObj interface{}) (err error) {
	if reqBody, err = json.Marshal(reqObj); err != nil {
		err = errors.Wrapf(err, "failed to encode reqObj: %+v", reqObj)
		return
	}
	var rsp *http.Response
	if rsp, err = ctl.hc.Post(servURL, "application/json", reqBody); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	var rspBody []byte
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
