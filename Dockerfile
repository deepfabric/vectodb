FROM centos:7 AS build_base

USER 0

RUN yum -y install centos-release-scl

RUN yum -y install devtoolset-8

RUN curl -o go.tgz https://mirrors.ustc.edu.cn/golang/go1.14.5.linux-amd64.tar.gz && tar -C /usr/local -xzf go.tgz

ENV GOPROXY=https://mirrors.aliyun.com/goproxy/,https://goproxy.cn,direct PATH=/usr/local/go/bin:${PATH} GOPATH=/root/go

RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN yum -y install git scons make openblas-devel swig python-devel numpy glog-devel gflags-devel jemalloc-devel

RUN mkdir -p $GOPATH/src/github.com/infinivision/vectodb
ADD . $GOPATH/src/github.com/infinivision/vectodb
WORKDIR $GOPATH/src/github.com/infinivision/vectodb

# https://unix.stackexchange.com/questions/530956/how-to-make-devtoolset-g-available-for-makefile-in-dockers-centos7
RUN source scl_source enable devtoolset-8 && scons -c && scons -j 8

FROM centos:7
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && yum clean all && rm -rf /var/cache/yum
RUN yum -y install glog gflags openblas-devel jemalloc && yum clean all && rm -rf /var/cache/yum
# Finally we copy the statically compiled Go binary.
COPY --from=build_base /root/go/src/github.com/infinivision/vectodb/demos/demo_sift1M_vectodb_go /usr/local/bin/demo_sift1M_vectodb_go
ENTRYPOINT ["/usr/local/bin/demo_sift1M_vectodb_go"]
