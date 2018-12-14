# package(default_visibility = ["//visibility:public"])

genrule(
    name = "build_go",
    srcs = glob([
        "*.go",
        "**/*.go",
        "go.mod",
        "go.sum",
        "/usr/local/go/bin/go",
    ]),
    building_description = "build go modules and binaries",
    cmd = [
        "GO111MODULE=on go install -x .",
    ],
    deps = [":build_faiss"]
)


for fp in ['demos/demo_sift1M.cpp', 'demos/faiss_train.cpp', 'demos/faiss_search.cpp', 'demos/generate_dataset.cpp']:
    cc_binary(
        name = splitext(basename(fp))[0],
        srcs = [fp],
        compiler_flags = ["--std=c++17"],
        linker_flags = ["-Lfaiss -lfaiss", "-lopenblas", "-lboost_filesystem", "-lboost_system", "-lgomp", "-lpthread"],
        deps = [":build_faiss"],
    )

for fp in ['demos/demo_sift1M_vectodb.cpp', 'demos/demo_sift100M_vectodb.cpp']:
    cc_binary(
        name = splitext(basename(fp))[0],
        srcs = [fp],
        hdrs = [
        "vectodb.h",
        "vectodb.hpp",
        ],
        compiler_flags = ["--std=c++17"],
        linker_flags = ["-L. -lvectodb", "-Lfaiss -lfaiss", "-lopenblas", "-lboost_thread", "-lboost_filesystem", "-lboost_system", "-lglog", "-lgflags", "-lgomp", "-lpthread"],
        deps = [":libvectodb", ":build_faiss"],
    )

cc_library(
    name = "libvectodb",
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
