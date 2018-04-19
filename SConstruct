import os
import os.path
import inspect
import subprocess

# Prepare faiss on CentOS 7 x86_64:
# 1. Enable EPEL.
# 2. Install dependencies.
# $ sudo yum -y install openblas-devel gtest-devel
# 3. Checkout faiss(https://github.com/facebookresearch/faiss.git) at faiss.
# 4. Build it:
# $ cd faiss
# $ cp example_makefiles/makefile.inc.Linux makefile.inc
# $ make demos/demo_sift1M

env = Environment()
env.Command('faiss/libfaiss.a', 'faiss/Makefile', 'pushd faiss && cp example_makefiles/makefile.inc.Linux makefile.inc && make demos/demo_sift1M && popd')
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

env.StaticLibrary('vectodb', ['vectodb.cpp'], LIBS=['boost_filesystem', 'boost_system'])


env.Command('demos/demo_sift1M_vectodb_go', ['demos/demo_sift1M_vectodb.go', 'vectodb.go', 'demos/demo_sift1M_vectodb'], 'go install -x . && pushd demos && go build -o demo_sift1M_vectodb_go demo_sift1M_vectodb.go && popd')

env.Alias('go', 'demos/demo_sift1M_vectodb_go')
