cmake_policy(SET CMP0048 NEW)
project(redisraft C)
cmake_minimum_required(VERSION 3.9)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

enable_testing()

add_compile_definitions(_GNU_SOURCE)

add_subdirectory(deps/libuv)
add_subdirectory(deps/raft)
add_subdirectory(deps/hiredis)
add_subdirectory(deps/cmocka)

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

target_link_libraries(redisraft PUBLIC raft uv hiredis)

target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/libuv/include>)
target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/raft/include>)
target_include_directories(redisraft PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/deps/>)

## ------------------------------ TEST ---------------------------------------##

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

add_custom_command(OUTPUT buildinfo WORKING_DIRECTORY
        ${PROJECT_SOURCE_DIR} COMMAND make buildinfo.h)
add_custom_target(info ALL DEPENDS buildinfo)

add_dependencies(main info)
add_test(NAME main COMMAND $<TARGET_FILE:main>)