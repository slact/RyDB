cmake_minimum_required(VERSION 3.0)
project("RyDB" VERSION 0.0.1 DESCRIPTION "fixed-size row database")

set(CMAKE_VERBOSE_MAKEFILE ON)

add_library(RyDB SHARED
  src/rydb.c
  src/rydb_hashtable.c
  src/rydb_transaction.c
  src/rbtree.c
)

set_target_properties(RyDB PROPERTIES VERSION ${PROJECT_VERSION})
target_include_directories(RyDB PRIVATE src)
