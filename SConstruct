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

env.Command('faiss/libfaiss.a', 'faiss/Makefile', 'pushd faiss && cp example_makefiles/makefile.inc.Linux makefile.inc && make demos/demo_sift1M && popd')

if env.GetOption('clean'):
    subprocess.call('pushd faiss && make clean && popd', shell=True)

selfPath = os.path.abspath(inspect.getfile(inspect.currentframe()))
mainDir, _ = os.path.split(selfPath)
faissDir = os.path.join(mainDir, 'faiss')
cpp_path = [mainDir]
libs_path = [mainDir, faissDir,'/usr/local/opt/libomp/lib']

env = Environment(ENV=os.environ, CPPPATH=cpp_path, LIBPATH=libs_path, PRJNAME="vectodb")
env.Append(CXXFLAGS='-Wall -Wextra -g -O2 -Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include -std=c++17')
env.Append(LINKFLAGS='-Xpreprocessor -fopenmp -L/usr/local/opt/libomp/lib -lomp')
#env.MergeFlags(env.ParseFlags('-Wall -Wextra -g -O2 -Xpreprocessor -fopenmp -std=c++17'))
Export("env")

SConscript(["demos/SConscript"])

env.StaticLibrary('vectodb', ['vectodb.cpp'], LIBS=['boost_thread-mt', 'boost_filesystem', 'boost_system'])
env.Append(LIBPATH='/usr/local/opt/boost/lib')

#env.Command('demos/demo_sift1M_vectodb_go', ['demos/demo_sift1M_vectodb.go', 'vectodb.go', 'demos/demo_sift1M_vectodb'], 'go install -x . && pushd demos && go build -o demo_sift1M_vectodb_go demo_sift1M_vectodb.go && popd')
