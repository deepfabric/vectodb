# package(default_visibility = ["//visibility:public"])

genrule(
    name = "build_go",
    srcs = glob([
        "*.go",
        "demos/*.go",
        "go.mod",
        "go.sum",
        "*.h",
        "*.hpp",
        "*.c",
        "*.cpp",
    ]),
    outs = [
        "demos/demo_sift1M_vectodb_go",
        "demos/demo_vectodblite_go",
    ],
    building_description = "build go modules and binaries",
    cmd = [
        "export GO111MODULE=on",
        "$TOOL install -x .",
        "$TOOL build -o demos/demo_sift1M_vectodb_go demos/demo_sift1M_vectodb.go",
        "$TOOL build -o demos/demo_vectodblite_go demos/demo_vectodblite.go",
    ],
    tools = ["/usr/local/go/bin/go"],
    deps = [":build_faiss"],
)

for fp in [
    "demos/demo_sift1M.cpp",
    "demos/faiss_train.cpp",
    "demos/faiss_search.cpp",
    "demos/generate_dataset.cpp",
]:
    cc_binary(
        name = splitext(basename(fp))[0],
        srcs = [fp],
        compiler_flags = ["--std=c++17"],
        linker_flags = ["-Lfaiss -lfaiss", "-lopenblas", "-lstdc++fs", "-lgomp", "-lpthread"],
        deps = [":build_faiss"],
    )

for fp in [
    "demos/demo_sift1M_vectodb.cpp",
]:
    cc_binary(
        name = splitext(basename(fp))[0],
        srcs = [fp],
        hdrs = [
            "vectodb.h",
            "vectodb.hpp",
        ],
        compiler_flags = ["--std=c++17"],
        linker_flags = ["-L. -lvectodb", "-Lfaiss -lfaiss", "-lopenblas", "-lstdc++fs", "-lglog", "-lgflags", "-lgomp", "-lpthread"],
        deps = [":build_faiss", ":libvectodb"],
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
        "((grep -i ubuntu /etc/os-release && cp faiss/example_makefiles/makefile.inc.Linux.Ubuntu faiss/makefile.inc) || cp faiss/example_makefiles/makefile.inc.Linux faiss/makefile.inc)",
        "make -C faiss demos/demo_sift1M",
    ],
)
