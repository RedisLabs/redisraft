# Library build :
#         mkdir build && cd build && cmake ..
#         make
# Coverage :
#         mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Coverage
#         make && make coverage
# Tests :
#         mkdir build && cd build && cmake ..
#         make && make tests
#
# Sanitizer:
#        mkdir build && cd build && cmake .. -DSANITIZER=address
#        make && make tests
# TLS:
#        mkdir build && cd build && cmake .. -DBUILD_TLS=1 -DPYTEST_OPTS="--tls -v"
#        make && make tests

cmake_policy(SET CMP0048 NEW)
project(redisraft C)
cmake_minimum_required(VERSION 3.7.2)

include(CheckCCompilerFlag)
include(TestBigEndian)

# ----------------------- Build Settings Start ------------------------------- #
set_property(GLOBAL PROPERTY ALLOW_DUPLICATE_CUSTOM_TARGETS 1)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

enable_testing()

if (NOT CMAKE_BUILD_TYPE)
    message(STATUS "No build type selected, defaulting to Release")
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif ()

message(STATUS "Main build type: ${CMAKE_BUILD_TYPE}")

# Detect endianness
test_big_endian(HAVE_BIG_ENDIAN)
if (${HAVE_BIG_ENDIAN})
    message(STATUS "System is BIG ENDIAN")
    target_compile_definitions(${PROJECT_NAME}_test PRIVATE -DHAVE_BIG_ENDIAN)
else ()
    message(STATUS "System is LITTLE ENDIAN")
endif ()

message(STATUS "CMAKE_SYSTEM_PROCESSOR is ${CMAKE_SYSTEM_PROCESSOR}")

# Detect architecture and set HAVE_CRC32C flag if supported by the CPU
if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64")
    message(STATUS "System is x86_64")

    # Extra check for sse4.2 support
    check_c_compiler_flag(-msse4.2 HAVE_CRC32_HARDWARE)
    if (${HAVE_CRC32_HARDWARE})
        message(STATUS "CPU have -msse4.2, defined HAVE_CRC32C")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAVE_CRC32C -msse4.2")
    endif ()
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
    message(STATUS "System is arm64")

    # Extra check for -march=armv8.1-a
    check_c_compiler_flag(-march=armv8.1-a HAVE_CRC32_HARDWARE)
    if (${HAVE_CRC32_HARDWARE})
        message(STATUS "CPU have -march=armv8.1-a, defined HAVE_CRC32C")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAVE_CRC32C -march=armv8.1-a")
    endif ()
endif ()

if (NOT HAVE_CRC32_HARDWARE)
    message(STATUS "CRC32C implementation will use the software version")
endif ()


set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_POSIX_C_SOURCE=200112L -D_GNU_SOURCE")

# Do not strip assert()'s from the code.
foreach (compile_flag
        CMAKE_C_FLAGS_RELEASE
        CMAKE_C_FLAGS_RELWITHDEBINFO
        CMAKE_C_FLAGS_MINSIZEREL)
    string(REPLACE "-DNDEBUG" "" "${compile_flag}" "${${compile_flag}}")
endforeach ()

if (BUILD_TLS)
    if (NOT OPENSSL_ROOT_DIR)
        if (APPLE)
            set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl")
        endif ()
    endif ()

    find_package(OpenSSL REQUIRED)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAVE_TLS")
endif ()

if (TRACE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_TRACE")
endif ()

if (NOT PYTEST_OPTS)
    set(PYTEST_OPTS "-v")
endif ()
# Create a PYTEST_OPTS list by replacing space with semicolon. Otherwise,
# Cmake thinks it has to escape space and inserts backslash.
string(REPLACE " " ";" PYTEST_LIST ${PYTEST_OPTS})

set(CMAKE_SHARED_MODULE_SUFFIX ".so")

if (SANITIZER)
    if ("${SANITIZER}" STREQUAL "address")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
        add_link_options(-fsanitize=address)
    elseif ("${SANITIZER}" STREQUAL "undefined")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
        add_link_options(-fsanitize=undefined)
    elseif ("${SANITIZER}" STREQUAL "thread")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
        add_link_options(-fsanitize=thread)
    else ()
        message(FATAL_ERROR "Unknown sanitizer : ${SANITIZER}")
    endif ()

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-sanitize-recover=all -fno-omit-frame-pointer")
    message(STATUS "Using sanitizer : ${SANITIZER}")
endif ()

# ------------------------ Build Settings End -------------------------------- #


# -------------------------- Dependencies Start ------------------------------ #

# --------------------------------- raft ------------------------------------- #
add_subdirectory(deps/raft)
set_property(TARGET raft PROPERTY POSITION_INDEPENDENT_CODE ON)
# ------------------------------- raft-end------------------------------------ #

# ------------------------------ hiredis ------------------------------------- #
if (BUILD_TLS)
    option(ENABLE_SSL "Building with TLS" ON)
endif ()
option(DISABLE_TESTS "Disable hiredis tests" ON)
option(BUILD_SHARED_LIBS "Link hiredis static lib" OFF)

# Silence hiredis warnings on MacOS
if (APPLE)
    set(CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> Scr <TARGET> <LINK_FLAGS> <OBJECTS>")
    set(CMAKE_C_ARCHIVE_FINISH "<CMAKE_RANLIB> -no_warning_for_no_symbols -c <TARGET>")
endif()

add_subdirectory(deps/hiredis)
set_property(TARGET hiredis PROPERTY POSITION_INDEPENDENT_CODE ON)

if (BUILD_TLS)
    set_property(TARGET hiredis_ssl PROPERTY POSITION_INDEPENDENT_CODE ON)
endif ()

# Disable warnings for dependencies
set_target_properties(hiredis PROPERTIES COMPILE_FLAGS "-w")
# ---------------------------- hiredis-end ----------------------------------- #

# ---------------------------- Dependencies End ------------------------------ #


# --------------------------- Build Helpers Start ---------------------------- #
add_custom_command(OUTPUT buildinfo
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src
        COMMAND export GIT_SHA1=` (git show-ref --head --hash=8 2>/dev/null || echo 00000000) | head -n1`
        && echo "\"\#define" "REDISRAFT_GIT_SHA1" "\\\"$$GIT_SHA1\\\"\"" > buildinfo.h)

add_custom_target(info ALL DEPENDS buildinfo)

set_property(TARGET info APPEND PROPERTY
        ADDITIONAL_CLEAN_FILES ${PROJECT_SOURCE_DIR}/src/buildinfo.h)
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
add_dependencies(coverage tests)

add_custom_target(coverage-report)
add_custom_command(
        TARGET coverage-report
        COMMAND mkdir -p tests/.lcov_html
        COMMAND genhtml --branch-coverage -o tests/.lcov_html coverage.info
        COMMAND xdg-open tests/.lcov_html/index.html >/dev/null 2>&1
)
add_dependencies(coverage-report coverage)
# ------------------------- Code Coverage End -------------------------------- #


# ------------------------ Library build start  ------------------------------ #
add_library(redisraft MODULE
        deps/common/crc16.c
        deps/common/sc_crc32.c
        deps/common/sc_list.c
        src/blocked.c
        src/clientstate.c
        src/cluster.c
        src/commands.c
        src/common.c
        src/config.c
        src/connection.c
        src/entrycache.c
        src/file.c
        src/fsync.c
        src/join.c
        src/log.c
        src/metadata.c
        src/migrate.c
        src/multi.c
        src/node.c
        src/node_addr.c
        src/proxy.c
        src/raft.c
        src/redisraft.c
        src/serialization.c
        src/serialization_utils.c
        src/snapshot.c
        src/sort.c
        src/threadpool.c
        src/util.c)

add_dependencies(redisraft info)
set_target_properties(redisraft PROPERTIES PREFIX "")
set_target_properties(redisraft PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}")
set_property(TARGET redisraft APPEND PROPERTY
        ADDITIONAL_CLEAN_FILES ${LIBRARY_OUTPUT_DIRECTORY}/$<TARGET_FILE_NAME:redisraft>)

target_compile_options(redisraft PRIVATE -Wall -Werror -Wextra -Wno-unused-parameter)

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(LINKER_FLAGS "-Wl,-Bsymbolic")
else ()
    set(LINKER_FLAGS "-Wl,-undefined -Wl,dynamic_lookup")
endif ()

set_property(TARGET redisraft PROPERTY LINK_FLAGS ${LINKER_FLAGS})

target_link_libraries(redisraft PUBLIC raft hiredis m)
if (BUILD_TLS)
    target_link_libraries(redisraft PUBLIC OpenSSL::SSL OpenSSL::Crypto hiredis_ssl)
endif ()

target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/raft/include>)
target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/common>)
target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/>)
# ------------------------- Library build end  ------------------------------- #


# ---------------------------- Test Start ------------------------------------ #
add_executable(main
        deps/common/crc16.c
        deps/common/sc_crc32.c
        deps/common/sc_list.c
        src/blocked.c
        src/clientstate.c
        src/cluster.c
        src/commands.c
        src/common.c
        src/config.c
        src/connection.c
        src/entrycache.c
        src/file.c
        src/fsync.c
        src/join.c
        src/log.c
        src/metadata.c
        src/migrate.c
        src/multi.c
        src/node.c
        src/node_addr.c
        src/proxy.c
        src/raft.c
        src/redisraft.c
        src/serialization.c
        src/serialization_utils.c
        src/snapshot.c
        src/sort.c
        src/threadpool.c
        src/util.c
        tests/unit/main.c
        tests/unit/test_file.c
        tests/unit/test_log.c
        tests/unit/test_serialization.c
        tests/unit/test_util.c)

target_compile_options(main PUBLIC -include unit/dut_premble.h)
target_link_libraries(main PRIVATE raft hiredis Threads::Threads dl m)
if (BUILD_TLS)
    target_link_libraries(main PRIVATE OpenSSL::SSL OpenSSL::Crypto hiredis_ssl)
endif ()

target_include_directories(main PUBLIC tests deps/raft/include deps/common deps/)

add_dependencies(main info)
add_test(NAME main COMMAND $<TARGET_FILE:main>)
set_property(TEST main PROPERTY LABELS redisraft-test)
add_custom_target(unit-tests ${CMAKE_COMMAND}
        -E env CTEST_OUTPUT_ON_FAILURE=1
        ${CMAKE_CTEST_COMMAND} -L redisraft -C $<CONFIG> --verbose
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

add_custom_target(integration-tests)
add_custom_command(TARGET integration-tests
        COMMAND pytest tests/integration ${PYTEST_LIST}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(valgrind-tests)
add_custom_command(TARGET valgrind-tests
        COMMAND pytest tests/integration ${PYTEST_LIST} --valgrind
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(tests)
add_dependencies(tests integration-tests unit-tests)

# ---------------------------- Test Modules ---------------------------------- #
macro(build_test_module name)
    add_library(${name} MODULE tests/integration/modules/${name}.c)
    set_target_properties(${name} PROPERTIES PREFIX "")
    target_include_directories(${name} PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/common/>)
    set_target_properties(${name} PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/tests/integration/modules")
    set_property(TARGET ${name} APPEND PROPERTY
            ADDITIONAL_CLEAN_FILES ${LIBRARY_OUTPUT_DIRECTORY}/$<TARGET_FILE_NAME:${name}>)
endmacro()

# Add new modules here.
#
# Example:
# Module file path: tests/integration/modules/hellomodule.c
# Output file path will be: tests/integration/modules/hellomodule.so
build_test_module(hellomodule)
# -------------------------- Test Modules End -------------------------------- #

# ----------------------------- Test End ------------------------------------- #
