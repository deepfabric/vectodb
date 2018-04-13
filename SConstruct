import os
import os.path
import inspect

selfPath = os.path.abspath(inspect.getfile(inspect.currentframe()))
mainDir, _ = os.path.split(selfPath)

cpp_path = [mainDir]
libs_path = [mainDir]

# if faiss is not installed to the system path, it need be provided as "Command-Line variable=value Build Variables", or an environment variable.
key = 'FAISS'
faiss_dir = ARGUMENTS.get(key, '')
if faiss_dir == '':
	if key in os.environ:
		faiss_dir = os.environ[key]

if faiss_dir != '':
	cpp_path.append(faiss_dir)
	libs_path.append(faiss_dir)

env = Environment(ENV=os.environ, CPPPATH=cpp_path, LIBPATH=libs_path, PRJNAME="vectodb")
env.MergeFlags(env.ParseFlags('-Wall -Wextra -g -O2 -fopenmp -std=c++17'))
Export("env")

SConscript(["demos/SConscript"])

env.StaticLibrary('vectodb', ['vectodb.cpp'], LIBS=['boost_filesystem', 'boost_system'])
