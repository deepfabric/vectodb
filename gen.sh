#!/bin/bash
#
# Generate all elasticell protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
DIRS="."

GOGOPROTO_ROOT="$(go env GOPATH)/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

for dir in ${DIRS}; do
	pushd ${dir}
		protoc --gofast_out=plugins=grpc:. -I=.:"${GOGOPROTO_PATH}":"$(go env GOPATH)/src" *.proto
	popd
done
