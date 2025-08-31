cmake_minimum_required(VERSION 3.10)
project("RyDB" "C")
set(DESCRIPTION "fixed-size row database")
set(VERSION 0.0.1)

#set(CMAKE_VERBOSE_MAKEFILE ON)
set(libsrc 
  src/rydb.c
  src/rydb_hashtable.c
  src/rydb_transaction.c
  src/rbtree.c
)

add_library(RyDB SHARED ${libsrc})

add_library(RyDB_static STATIC ${libsrc})
set_target_properties(RyDB_static PROPERTIES OUTPUT_NAME RyDB)

set_target_properties(RyDB PROPERTIES VERSION ${VERSION})
target_include_directories(RyDB PRIVATE src)
target_include_directories(RyDB_static PRIVATE src)

include(CheckSymbolExists)
include(TestBigEndian)
include(CMakePushCheckState)
include(GNUInstallDirs)

set(CMAKE_MODULE_PATH "./")

cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
check_symbol_exists(mremap "sys/mman.h" RYDB_HAVE_MREMAP)
cmake_reset_check_state()

find_package(atomic_ops MODULE REQUIRED)

TEST_BIG_ENDIAN(RYDB_BIG_ENDIAN)

file(TO_NATIVE_PATH "/" RYDB_PATH_SEPARATOR)

target_include_directories(RyDB PUBLIC "${LIBATOMIC_OPS_INCLUDE_DIR}")
target_link_libraries(RyDB "${LIBATOMIC_OPS_LIBRARY}")

target_include_directories(RyDB_static PUBLIC "${LIBATOMIC_OPS_INCLUDE_DIR}")
target_link_libraries(RyDB_static "${LIBATOMIC_OPS_LIBRARY}")

configure_file(src/configure.h.tmpl src/configure.h)
