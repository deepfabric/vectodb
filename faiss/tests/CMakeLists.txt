file(GLOB srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

# Build each source file independently
include_directories(../../)	# faiss root directory

# gtest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})
link_directories("/usr/local/opt/openblas/lib")
foreach(source ${srcs})
    get_filename_component(name ${source} NAME_WE)

    # target
    add_executable(${name} ${source})
    target_link_libraries(${name} ${faiss_lib} -lopenblas ${GTEST_BOTH_LIBRARIES})

    # Install
    install(TARGETS ${name} DESTINATION test)
endforeach(source)
