FROM centos/devtoolset-7-toolchain-centos7 AS build_base

USER 0

RUN curl -o go.tgz https://dl.google.com/go/go1.11.4.linux-amd64.tar.gz && tar -C /usr/local -xzf go.tgz

RUN yum -y install git 

ENV PATH=/usr/local/go/bin:${PATH}

RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN yum -y install scons make openblas-devel swig python-devel numpy glog-devel gflags-devel boost-devel jemalloc-devel

# https://container-solutions.com/faster-builds-in-docker-with-go-1-11/
# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .
RUN go mod download

RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN scons -c && scons

FROM centos:7
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && yum clean all && rm -rf /var/cache/yum
RUN yum -y install boost-thread boost-system boost-filesystem glog gflags openblas-devel jemalloc && yum clean all && rm -rf /var/cache/yum
# Finally we copy the statically compiled Go binary.
COPY --from=build_base /app/cmd/vectodblite_cluster/vectodblite_cluster /usr/local/bin/vectodblite_cluster
ENTRYPOINT ["/usr/local/bin/vectodblite_cluster"]
