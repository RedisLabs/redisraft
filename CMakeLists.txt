project(raft C)
cmake_minimum_required(VERSION 3.9)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

enable_testing()

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

add_library(raft STATIC
        src/raft_log.c
        src/raft_server.c
        src/raft_node.c
        src/raft_server_properties.c)

target_compile_options(raft PRIVATE -fPIC -g)
target_include_directories(raft PRIVATE include)

define_test(test_log)
define_test(test_log_impl)
define_test(test_node)
define_test(test_scenario)
define_test(test_server)
define_test(test_snapshotting)
