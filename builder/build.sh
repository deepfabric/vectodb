#!/usr/bin/env bash

sudo docker build --build-arg http_proxy=http://172.19.0.101:8118 --build-arg https_proxy=http://172.19.0.101:8118 --network=host .
