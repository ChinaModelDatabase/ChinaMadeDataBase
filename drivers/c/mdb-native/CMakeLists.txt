cmake_minimum_required(VERSION 3.14)
project(mdb_native C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(SOURCES
    src/bson.c
    src/wire.c
    src/crc32c.c
    src/connection.c
    src/client.c
    src/cursor.c
)

add_library(mdb_native STATIC ${SOURCES})
target_include_directories(mdb_native PUBLIC include)

if(UNIX)
    target_link_libraries(mdb_native PRIVATE m)
    target_compile_definitions(mdb_native PRIVATE MDB_PLATFORM_POSIX)
elseif(WIN32)
    target_link_libraries(mdb_native PRIVATE ws2_32)
    target_compile_definitions(mdb_native PRIVATE MDB_PLATFORM_WIN32)
endif()

add_executable(mdb_native_tests tests/test_native.c)
target_link_libraries(mdb_native_tests PRIVATE mdb_native)

add_executable(mdb_native_example examples/basic_usage.c)
target_link_libraries(mdb_native_example PRIVATE mdb_native)

install(TARGETS mdb_native
    ARCHIVE DESTINATION lib
)
install(DIRECTORY include/mdb_native DESTINATION include)
