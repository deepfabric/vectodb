# package(default_visibility = ["//visibility:public"])

for fp in glob(["demos/*.cpp"]):
    cc_binary(
        name = splitext(basename(fp))[0],
        srcs = [fp],
        hdrs = [
        "vectodb.h",
        "vectodb.hpp",
        ],
        compiler_flags = ["--std=c++17"],
        deps = [":lib_vectodb", ":openblas"],
    )


cc_library(
    name = "openblas",
    srcs = ["/usr/lib64/libopenblas.so.0"],
)

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
        "--std=c++17",
        "-fopenmp",
    ],
    includes = ["faiss"],
    deps = [":build_faiss"],
)

genrule(
    name = "build_faiss",
    srcs = glob([
        "faiss/Makefile",
        "faiss/**/*.cpp",
        "faiss/**/*.h",
        "faiss/**/*makefile*",
    ]),
    outs = ["faiss/libfaiss.a"] + glob(["faiss/**/*.h"]),
    binary = False,
    building_description = "build fiass static library",
    cmd = [
        "cp faiss/example_makefiles/makefile.inc.Linux faiss/makefile.inc",
        "make -C faiss demos/demo_sift1M",
    ],
)
