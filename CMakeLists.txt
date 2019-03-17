cmake_minimum_required(VERSION 3.0)
project("RyDB")
set(DESCRIPTION "fixed-size row database")
set(VERSION 0.0.1)
set(LANGUAGES "C")

set(CMAKE_VERBOSE_MAKEFILE ON)
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
