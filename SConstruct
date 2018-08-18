import os
import os.path
import inspect
import subprocess

# Build vectodb on CentOS 7 x86_64:
# 1. Enable EPEL, refers to https://fedoraproject.org/wiki/EPEL.
# 2. Install dependencies.
# $ sudo yum -y install gcc-g++ openblas-devel swig python-devel numpy glog-devel gflags-devel boost-devel
# 3. Install and enable devtoolset-7, refers to https://www.softwarecollections.org/en/scls/rhscl/devtoolset-7/, https://access.redhat.com/solutions/527703
# 4. Build all
# $ scons

env = Environment(ENV=os.environ)

env.Command('faiss/libfaiss.a', 'faiss/Makefile', 'pushd faiss && cp example_makefiles/makefile.inc.Linux makefile.inc && make demos/demo_sift1M py && popd')
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


env.Command('demos/demo_sift1M_vectodb_go', ['demos/demo_sift1M_vectodb.go', 'vectodb.go', 'demos/demo_sift1M_vectodb'], 'GO111MODULE=on go install -x . && pushd demos && GO111MODULE=on go build -o demo_sift1M_vectodb_go demo_sift1M_vectodb.go && popd')
