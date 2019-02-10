#include "test_util.h"
#include <stdlib.h>


//wonky memory allocator
static uint64_t malloc_n, malloc_n_max = UINT64_MAX;
static rydb_allocator_t testallocator;
void *testmalloc(size_t sz) {
  malloc_n++;
  if(malloc_n > malloc_n_max) {
    return NULL;
  }
  return malloc(sz);
}
void *testrealloc(void *ptr, size_t sz) {
  malloc_n++;
  if(malloc_n > malloc_n_max) {
    return NULL;
  }
  return realloc(ptr, sz);
}
void fail_malloc_after(int n) {
  malloc_n = 0;
  malloc_n_max = n;
  rydb_global_config_allocator(&testallocator);
}
void reset_malloc(void) {
  rydb_global_config_allocator(NULL);
}
static rydb_allocator_t testallocator = {
  testmalloc, testrealloc, free
};
