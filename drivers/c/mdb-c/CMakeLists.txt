cmake_minimum_required(VERSION 3.16)
project(mdb-c VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(PkgConfig)
pkg_check_modules(MONGOC REQUIRED libmongoc-1.0)

add_library(mdb STATIC src/mdb.c)
target_include_directories(mdb PUBLIC include ${MONGOC_INCLUDE_DIRS})
target_link_libraries(mdb ${MONGOC_LIBRARIES})

add_library(mdb_cpp STATIC src/mdb.cpp)
target_include_directories(mdb_cpp PUBLIC include ${MONGOC_INCLUDE_DIRS})
target_link_libraries(mdb_cpp mdb ${MONGOC_LIBRARIES})

enable_testing()
add_executable(mdb_test tests/test_mdb.c)
target_link_libraries(mdb_test mdb ${MONGOC_LIBRARIES})
add_test(NAME mdb_test COMMAND mdb_test)

add_executable(basic_usage examples/basic_usage.c)
target_link_libraries(basic_usage mdb ${MONGOC_LIBRARIES})
