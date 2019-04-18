FROM centos/devtoolset-7-toolchain-centos7 AS build_base

USER 0

RUN curl -o go.tgz https://dl.google.com/go/go1.12.4.linux-amd64.tar.gz && tar -C /usr/local -xzf go.tgz

RUN yum -y install git 

ENV PATH=/usr/local/go/bin:${PATH}

RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN yum -y install scons make openblas-devel swig python-devel numpy glog-devel gflags-devel boost-devel jemalloc-devel

ENV GOPATH=$HOME/go
RUN mkdir -p $GOPATH/src/github.com/infinivision/vectodb
ADD . $GOPATH/src/github.com/infinivision/vectodb
WORKDIR $GOPATH/src/github.com/infinivision/vectodb
RUN scons -c && scons

FROM centos:7
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && yum clean all && rm -rf /var/cache/yum
RUN yum -y install boost-thread boost-system boost-filesystem glog gflags openblas-devel jemalloc && yum clean all && rm -rf /var/cache/yum
# Finally we copy the statically compiled Go binary.
COPY --from=build_base /opt/app-root/src/go/src/github.com/infinivision/vectodb/cmd/vectodblite_cluster/vectodblite_cluster /usr/local/bin/vectodblite_cluster
ENTRYPOINT ["/usr/local/bin/vectodblite_cluster"]
