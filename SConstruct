import os
import os.path
import inspect
import subprocess
import glob

# Build vectodb on CentOS 7 x86_64:
# 1. Enable EPEL, refers to https://fedoraproject.org/wiki/EPEL.
# 2. Install dependencies.
# $ sudo yum -y install gcc-c++ openblas-devel swig python-devel numpy glog-devel gflags-devel boost-devel jemalloc-devel
# 3. Install and enable devtoolset-8, refers to https://www.softwarecollections.org/en/scls/rhscl/devtoolset-8/, https://access.redhat.com/solutions/527703
# 4. Build all
# $ scons

# Build vectodb on Linux Mint 19 x86_64:
# 1. Change default shell from dash to bash.
# $ sudo dpkg-reconfigure dash
# select No.
# 2. Install dependencies.
# $ sudo apt install libboost-dev libboost-thread-dev libboost-system-dev libboost-filesystem-dev libopenblas-dev libgoogle-glog-dev libjemalloc-dev
# 3. Build all
# $ scons

env = Environment(ENV=os.environ)

env.Command('faiss/libfaiss.a', 'faiss/Makefile', 'pushd faiss && ./configure --disable-openmp --without-cuda && make -j8 && popd')
if env.GetOption('clean'):
    subprocess.call('pushd faiss && make clean && popd', shell=True)

selfPath = os.path.abspath(inspect.getfile(inspect.currentframe()))
mainDir, _ = os.path.split(selfPath)
faissDir = os.path.join(mainDir, 'faiss')
cpp_path = [mainDir]
libs_path = [mainDir, faissDir]

env = Environment(ENV=os.environ, CPPPATH=cpp_path, LIBPATH=libs_path, PRJNAME="vectodb")
env.MergeFlags(env.ParseFlags('-Wall -Wextra -g -O2 -fopenmp -std=c++17'))
Export("env")

SConscript(["demos/SConscript"])

env.StaticLibrary('vectodb', ['vectodb.cpp'], LIBS=['boost_thread', 'boost_filesystem', 'boost_system'])

env.Command('demos/demo_sift1M_vectodb_go', glob.glob('*.go') + glob.glob('demos/*.go') + glob.glob('*.cpp') + ['faiss/libfaiss.a'], 'go install -x . && pushd demos && go build -o demo_sift1M_vectodb_go demo_sift1M_vectodb.go && go build -o demo_sift100M_vectodb_go demo_sift100M_vectodb.go && go build -o demo_vectodblite_go demo_vectodblite.go && popd')

env.Command('cmd/vectodblite_cluster/vectodblite_cluster', glob.glob('cmd/vectodblite_cluster/*.go') + ['demos/demo_sift1M_vectodb_go'], 'pushd cmd/vectodblite_cluster && go build . && popd')

env.Command('cmd/vdblc_bb/vdblc_bb', glob.glob('cmd/vdblc_bb/*.go') + ['cmd/vectodblite_cluster/vectodblite_cluster'], 'pushd cmd/vdblc_bb && go build . && popd')
