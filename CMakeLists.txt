# Library build :
#         mkdir build && cd build && cmake ..
#         make
# Coverage :
#         mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Coverage
#         make && make coverage
# Tests :
#         mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug
#         make && make tests
#
# Sanitizer:
#        mkdir build && cd build && cmake .. -DSANITIZER=address -DCMAKE_BUILD_TYPE=Debug
#        make && make tests

cmake_policy(SET CMP0048 NEW)
project(redisraft C)
cmake_minimum_required(VERSION 3.9)


# ----------------------- Build Settings Start ------------------------------- #
set_property(GLOBAL PROPERTY ALLOW_DUPLICATE_CUSTOM_TARGETS 1)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

enable_testing()

if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, defaulting to Release")
    set(CMAKE_BUILD_TYPE "Release")
endif ()

message(STATUS "Main build type: ${CMAKE_BUILD_TYPE}")

add_compile_definitions(_GNU_SOURCE)

if (DEFINED ENV{TRACE})
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_TRACE")
endif()

if (SANITIZER)
    if ("${SANITIZER}" STREQUAL "address")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
        add_link_options(-fsanitize=address)
    elseif("${SANITIZER}" STREQUAL "undefined")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
        add_link_options(-fsanitize=undefined)
    elseif("${SANITIZER}" STREQUAL "thread")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
        add_link_options(-fsanitize=thread)
    else()
        message(FATAL_ERROR "Unknown sanitizer : ${SANITIZER}")
    endif()

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-sanitize-recover=all -fno-omit-frame-pointer")
    link_libraries(-static-libasan)
    message(STATUS "Using sanitizer : ${SANITIZER}")
endif()

# ------------------------ Build Settings End -------------------------------- #


# -------------------------- Dependencies Start ------------------------------ #
add_subdirectory(deps/libuv)
add_subdirectory(deps/raft)

option(UNIT_TESTING "Disable cmocka tests" OFF)
option(WITH_EXAMPLES "Disable cmocka examples" OFF)
add_subdirectory(deps/cmocka)

option(DISABLE_TESTS "Disable hiredis tests" ON)
add_subdirectory(deps/hiredis)
# ---------------------------- Dependencies End ------------------------------ #


# --------------------------- Build Helpers Start ---------------------------- #
add_custom_command(OUTPUT buildinfo
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND export GIT_SHA1=` (git show-ref --head --hash=8 2>/dev/null || echo 00000000) | head -n1`
        && echo "\"\#define\$ REDISRAFT_GIT_SHA1\$ \\\"$$GIT_SHA1\\\"\"" > buildinfo.h)

add_custom_target(info ALL DEPENDS buildinfo)

add_custom_target(check ${CMAKE_COMMAND}
        -E env CTEST_OUTPUT_ON_FAILURE=1
        ${CMAKE_CTEST_COMMAND} -C $<CONFIG> --verbose
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
# ---------------------------- Build Helpers End ----------------------------- #


# -------------------------- Code Coverage Start ----------------------------- #
if (${CMAKE_BUILD_TYPE} MATCHES "Coverage")
    add_compile_options(-fprofile-arcs -ftest-coverage)
    link_libraries(gcov)
endif ()

add_custom_target(coverage)
add_custom_command(
        TARGET coverage
        COMMAND lcov --capture --directory .
        --output-file coverage.info --rc lcov_branch_coverage=1 --rc lcov_excl_br_line='assert'
        COMMAND lcov --remove coverage.info '/usr/*' '*example*' '*test*' '*deps*'
        --output-file coverage.info --rc lcov_branch_coverage=1 --rc lcov_excl_br_line='assert'
        COMMAND lcov --list coverage.info --rc lcov_branch_coverage=1 --rc lcov_excl_br_line='assert'
)

add_dependencies(coverage check)
# ------------------------- Code Coverage End -------------------------------- #


# ------------------------ Library build start  ------------------------------ #
add_library(redisraft SHARED
        cluster.c
        commands.c
        common.c
        config.c
        connection.c
        crc16.c
        crc16.h
        join.c
        log.c
        node.c
        node_addr.c
        proxy.c
        raft.c
        redisraft.c
        serialization.c
        snapshot.c
        util.c)

add_dependencies(redisraft info)
target_link_libraries(redisraft PUBLIC raft uv hiredis)

target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/libuv/include>)
target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/raft/include>)
target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/>)
# ------------------------- Library build end  ------------------------------- #


# ---------------------------- Test Start ------------------------------------ #
add_executable(main
        cluster.c
        commands.c
        common.c
        config.c
        connection.c
        crc16.c
        crc16.h
        join.c
        log.c
        node.c
        node_addr.c
        proxy.c
        raft.c
        redisraft.c
        serialization.c
        snapshot.c
        util.c
        tests/main.c
        tests/test_log.c
        tests/test_serialization.c
        tests/test_util.c)

target_compile_options(main PUBLIC -include dut_premble.h)
target_link_libraries(main PRIVATE cmocka uv raft hiredis)
target_include_directories(main PUBLIC
        tests deps/raft/include deps/libuv/include deps/)

add_dependencies(main info)
add_test(NAME main COMMAND $<TARGET_FILE:main>)

if (NOT DEFINED ENV{PYTEST_OPTS})
    set(ENV{PYTEST_OPTS} "-v")
endif()

add_custom_target(integration-tests)
add_custom_command(TARGET integration-tests
        COMMAND pytest tests/integration $(PYTEST_OPTS)
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(valgrind-tests)
add_custom_command(TARGET valgrind-tests
        COMMAND pytest tests/integration $(PYTEST_OPTS) --valgrind
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(tests)
add_dependencies(tests check integration-tests)
# ----------------------------- Test End ------------------------------------- #
