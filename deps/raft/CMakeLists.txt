# Library build :
#         mkdir build && cd build && cmake ..
#         make
# Coverage :
#         mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Coverage
#         make && make lib_coverage
# Tests :
#         mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug
#         make && make tests_full

project(raft C)
cmake_minimum_required(VERSION 3.7.2)

macro(define_test name)
    add_executable(${name}
            tests/${name}.c
            tests/CuTest.c
            tests/linked_list_queue.c
            tests/mock_send_functions.c)

    target_link_libraries(${name} raft)
    target_include_directories(${name} PRIVATE include/)
    add_test(NAME ${name} COMMAND $<TARGET_FILE:${name}>)
endmacro()

# ----------------------- Build Settings Start ------------------------------- #
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

enable_testing()

if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, defaulting to Release")
    set(CMAKE_BUILD_TYPE "Release")
endif ()

message(STATUS "Main build type: ${CMAKE_BUILD_TYPE}")
# ------------------------ Build Settings End -------------------------------- #


# --------------------------- Build Helpers Start ---------------------------- #
add_custom_target(check ${CMAKE_COMMAND}
        -E env CTEST_OUTPUT_ON_FAILURE=1
        ${CMAKE_CTEST_COMMAND} -C $<CONFIG> --verbose
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
# ---------------------------- Build Helpers End ----------------------------- #


# --------------------------- Code Coverage Start ---------------------------- #
if (${CMAKE_BUILD_TYPE} MATCHES "Coverage")
    add_compile_options(-fprofile-arcs -ftest-coverage)
    link_libraries(gcov)
endif ()

add_custom_target(lib_coverage)
add_custom_command(
        TARGET lib_coverage
        COMMAND lcov --capture --directory .
        --output-file coverage.info --rc lcov_branch_coverage=1 --rc lcov_excl_br_line='assert'
        COMMAND lcov --remove coverage.info '/usr/*' '*example*' '*test*'
        --output-file coverage.info --rc lcov_branch_coverage=1 --rc lcov_excl_br_line='assert'
        COMMAND lcov --list coverage.info --rc lcov_branch_coverage=1 --rc lcov_excl_br_line='assert'
)

add_dependencies(lib_coverage check test_fuzzer test_virtraft)
# ---------------------------- Code Coverage End ----------------------------- #


# ------------------------------ Python Tests ---------------------------------#

set(LIBPATH PYTHONPATH=${CMAKE_BINARY_DIR}/tests LD_LIBRARY_PATH=${CMAKE_BINARY_DIR})

add_custom_target(test_fuzzer)
add_custom_command(TARGET test_fuzzer
                   COMMAND ${LIBPATH} python ${CMAKE_SOURCE_DIR}/tests/log_fuzzer.py
                   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_dependencies(test_fuzzer raft_cffi)

add_custom_target(test_virtraft)
add_custom_command(TARGET test_virtraft
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 5 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 1 -m 3 $(VIRTRAFT_OPTS)
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 7 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 1 -m 3 $(VIRTRAFT_OPTS)
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 5 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 2 -m 3 $(VIRTRAFT_OPTS)
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 5 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 3 -m 3 $(VIRTRAFT_OPTS)
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 5 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 4 -m 3 $(VIRTRAFT_OPTS)
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 5 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 5 -m 3 $(VIRTRAFT_OPTS)
        COMMAND ${LIBPATH} python3 ${CMAKE_SOURCE_DIR}/tests/virtraft2.py --servers 5 -i 20000 --compaction_rate 50 --drop_rate 5 -P 10 --seed 6 -m 3 $(VIRTRAFT_OPTS))
add_dependencies(test_virtraft raft_cffi)

add_custom_target(raft_cffi)
add_custom_command(TARGET raft_cffi
                   COMMAND python3 ${CMAKE_SOURCE_DIR}/tests/raft_cffi_builder.py --libname raft_shared --libdir ${CMAKE_BINARY_DIR} --includedir ${CMAKE_SOURCE_DIR}/include --tmpdir ${CMAKE_BINARY_DIR}/
                   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
# ----------------------------- Python Tests End ----------------------------- #


# ----------------------------- C Tests Start -------------------------------- #
add_custom_target(tests_full)
add_dependencies(tests_full check test_fuzzer test_virtraft)

set(RAFT_SOURCE_FILES
        src/raft_log.c
        src/raft_server.c
        src/raft_node.c
        src/raft_server_properties.c)

add_library(raft STATIC ${RAFT_SOURCE_FILES})
target_compile_options(raft PRIVATE -g -Wall -Wextra -pedantic)
target_include_directories(raft PRIVATE include)

add_library(raft_shared SHARED ${RAFT_SOURCE_FILES})
target_include_directories(raft_shared PRIVATE include)
set_property(TARGET raft_shared PROPERTY POSITION_INDEPENDENT_CODE ON)
target_compile_options(raft_shared PRIVATE -g -Wall -Wextra -pedantic)

define_test(test_log)
define_test(test_log_impl)
define_test(test_node)
define_test(test_scenario)
define_test(test_server)
define_test(test_snapshotting)
# ------------------------------ C Tests End --------------------------------- #

