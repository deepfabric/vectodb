# package(default_visibility = ["//visibility:public"])

cc_library(
    name = "lib_vectodb",
    srcs = [
        "vectodb.cpp",
    ],
    hdrs = [
        "vectodb.h",
        "vectodb.hpp",
    ],
    compiler_flags = [
        "-Wall",
        "-Wextra",
        "-g",
        "-O2",
        "-fopenmp",
        "-std=c++17",
    ],
    includes = ["faiss"],
    deps = [":build_faiss"],
)

genrule(
    name = "build_faiss",
    srcs = glob(include = [
        "faiss/Makefile",
        "faiss/**/*.cpp",
        "faiss/**/*.h",
        "faiss/**/*makefile*",
    ]),
    outs = ["faiss/libfaiss.a"] + glob(include=["faiss/**/*.h"]),
    binary = False,
    building_description = "build fiass static library",
    cmd = [
        "cp faiss/example_makefiles/makefile.inc.Linux faiss/makefile.inc",
        "make -C faiss demos/demo_sift1M",
    ],
)
