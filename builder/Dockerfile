FROM centos/devtoolset-7-toolchain-centos7

USER 0

RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

RUN yum -y install which scons make gcc-g++ openblas-devel swig python-devel numpy glog-devel gflags-devel boost-devel

COPY go1.10.3.linux-amd64.tar.gz /usr/local

RUN cd /usr/local && tar xzvf go1.10.3.linux-amd64.tar.gz

ENV PATH=/usr/local/go/bin:${PATH}
