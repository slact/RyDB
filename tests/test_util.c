#define _XOPEN_SOURCE 700
#include "test_util.h"
#include <stdlib.h>
#include <ftw.h>

#if RYDB_DEBUG
char intercepted_printf_buf[4096];

int intercepted_printf( const char * format, ... ) {
  int rc;
  va_list ap;
  memset(intercepted_printf_buf, '-', 4096);
  va_start(ap, format);
  rc = vsnprintf(intercepted_printf_buf, 4096, format, ap);
  va_end(ap);
  return rc;
}
int intercepted_fprintf( FILE * stream, const char * format, ... ) {
  (void) (stream);
  int rc;
  va_list ap;
  memset(intercepted_printf_buf, '-', 4096);
  va_start(ap, format);
  rc = vsnprintf(intercepted_printf_buf, 4096, format, ap);
  va_end(ap);
  return rc;
}
int rydb_intercept_printfs(void) {
  rydb_printf = intercepted_printf;
  rydb_fprintf = intercepted_fprintf;
  return 1;
}
int rydb_unintercept_printfs(void) {
  rydb_printf = printf;
  rydb_fprintf = fprintf;
  return 1;
}
#endif

int sed_meta_file(rydb_t *db, char *regex) {
  char cmd[1024];
  
  //sprintf(cmd, "sed -r -e \"%s\" %s", regex, db->meta.path);
  //system(cmd);
  sprintf(cmd, "sed -e \"%s\" %s > %s.tmp", regex, db->meta.path, db->meta.path);
  system(cmd);
  sprintf(cmd, "mv %s.tmp %s", db->meta.path, db->meta.path);
  system(cmd);
  return 1;
}

int is_little_endian(void) {
  volatile union {
    uint8_t  c[4];
    uint32_t i;
  } u;
  u.i = 0x01020304;
  return u.c[0] == 0x04;
}

int sed_meta_file_prop(rydb_t *db, const char *prop, const char *val) {
  char regex[128];
  sprintf(regex,"s/%s: .*/%s: %s/", prop, prop, val);
  return sed_meta_file(db, regex);
}

int rydb_reopen(rydb_t **db) {
  char path[1023], name[1024];
  strcpy(path, (*db)->path);
  strcpy(name, (*db)->name);
  rydb_close(*db);
  *db = rydb_new();
  return rydb_open(*db, path, name);
}

void config_testdb(rydb_t *db, int rowlen) {
  if(rowlen==0) {
    rowlen = ROW_LEN;
  }
  int row_index_len = rowlen < ROW_INDEX_LEN ? rowlen : ROW_INDEX_LEN;
  assert_db_ok(db, rydb_config_row(db, rowlen, row_index_len));
  if(rowlen > 10) {
    assert_db_ok(db, rydb_config_add_index_hashtable(db, "foo", 5, 5, RYDB_INDEX_DEFAULT, NULL));
  }
  if(rowlen > 15) {
    assert_db_ok(db, rydb_config_add_index_hashtable(db, "bar", 10, 5, RYDB_INDEX_DEFAULT, NULL));
  }
  assert_db_ok(db, rydb_config_add_row_link(db, "next", "prev"));
  assert_db_ok(db, rydb_config_add_row_link(db, "fwd", "rew"));
}

int ___rydb_failed_as_expected(rydb_t *db, char *callstr, int rc, rydb_error_code_t expected_error_code, const char *errmsg_match, char *errmsg_result) {
  rydb_error_t *err = rydb_error(db);
  if(rc == 1) {
    snprintf(errmsg_result, MAXERRLEN, "Expected %s to fail with error %s [%i], but succeeded instead", callstr, rydb_error_code_str(expected_error_code), expected_error_code);
    return 0;
  }
  if(!err) {
    snprintf(errmsg_result, MAXERRLEN, "Expected %s to fail with error %s [%i], but failed without setting an error instead", callstr, rydb_error_code_str(expected_error_code), expected_error_code);
    return 0;
  }
  char buf[900];
  rydb_error_snprint(db, buf, 1024);
  if(err->code != expected_error_code) {
    snprintf(errmsg_result, MAXERRLEN, "Expected %s to fail with error %s [%i], but instead got %s", callstr, rydb_error_code_str(expected_error_code), expected_error_code, buf);
    return 0;
  }
  if(strlen(errmsg_match) > 0) {
    regex_t   regex;
    int       rcx;
    char      rxerrbuf[100];
    rcx = regcomp(&regex, errmsg_match, REG_EXTENDED);
    if (rcx != 0) {
      snprintf(errmsg_result, MAXERRLEN, "Bad regex %s", errmsg_match);
      return 0;
    }
    rcx = regexec(&regex, buf, 0, NULL, 0);
    if(rcx == REG_NOMATCH) {
      snprintf(errmsg_result, MAXERRLEN, "Expected %s to fail with error %s [%i] matching /%s/, but instead got %s", callstr, rydb_error_code_str(expected_error_code), expected_error_code, errmsg_match, buf);
      regfree(&regex);
      return 0;
    }
    if(rcx != 0) {
      regerror(rcx, &regex, rxerrbuf, 100);
      snprintf(errmsg_result, MAXERRLEN, "Expected %s to fail with error %s [%i] matching %s, but instead got regex error %s", callstr, rydb_error_code_str(expected_error_code), expected_error_code, errmsg_match, rxerrbuf);
      regfree(&regex);
      return 0;
    }
    regfree(&regex);
    return 1;
  } 
  return 1;
}


//wonky memory allocator
static uint64_t malloc_n, malloc_n_max = UINT64_MAX;
static uint_fast8_t  increment_and_reset_on_malloc_fail = 0;
static rydb_allocator_t testallocator;

void *fail_allocation(void) {
  if(increment_and_reset_on_malloc_fail) {
    malloc_n_max++;
    malloc_n = 0;
  }
  return NULL;
}
void *testmalloc(size_t sz) {
  malloc_n++;
  if(malloc_n > malloc_n_max) {
    return fail_allocation();
  }
  return malloc(sz);
}
void *testrealloc(void *ptr, size_t sz) {
  malloc_n++;
  if(malloc_n > malloc_n_max) {
    return fail_allocation();
  }
  return realloc(ptr, sz);
}
void fail_malloc_after(int n) {
  malloc_n = 0;
  malloc_n_max = n;
  increment_and_reset_on_malloc_fail = 0;
  rydb_global_config_allocator(&testallocator);
}
void reset_malloc(void) {
  rydb_global_config_allocator(NULL);
}
static rydb_allocator_t testallocator = {
  testmalloc, testrealloc, free
};


void fail_malloc_later_each_time(void) {
  fail_malloc_after(0);
  increment_and_reset_on_malloc_fail = 1;
}


static int rmFiles(const char *pathname, const struct stat *sbuf, int type, struct FTW *ftwb) {
  (void)(type);
  (void)(ftwb);
  (void)(sbuf);
  if(remove(pathname) < 0) return -1;
  return 0;
}

int rmdir_recursive(const char *path) {
  if (nftw(path, rmFiles,10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS) < 0) {
    return 0;
  }
  return 1;
}

static int counted_files;
static int filecounter(const char *pathname, const struct stat *sbuf, int type, struct FTW *ftwb) {
  (void)(type);
  (void)(ftwb);
  (void)(sbuf);
  (void)(pathname);
  counted_files++;
  return 0;
}
int count_files(const char *path) {
  counted_files = 0;
  nftw(path, filecounter,10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS);
  return counted_files;
}

off_t filesize(const char *filename) {
  struct stat st;
  if (stat(filename, &st) == 0)
    return st.st_size;
  return -1; 
}

void cmd_rownum_out_of_range_check(rydb_t *db, struct cmd_rownum_out_of_range_check_s *check, int nrows) {
  int badrownums[] = {0, nrows + 1000, nrows + 2};
  for(int j = 0; j<check->n_check; j++) {
    rydb_row_t *row = &check->rows[j];
    for(int k = 0; k<3; k++) {
      row->num = badrownums[k];
      assert_db_ok(db, rydb_transaction_start(db));
      assert_db_ok(db, rydb_data_append_cmd_rows(db, check->rows, check->n));
      char match[128];
      snprintf(match, 128, "%s.* failed.* rownum.* %s", check->name, k<2 ? "out of range" : "beyond command");
      //printf("%s %i %i %i\n", check->name, i, j, k);
      //rydb_print_stored_data(db);
      assert_db_fail_match_errstr(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, match);
      row->num = 1;
    }
  }
}

void hashtable_header_count_check(const rydb_t *db, const rydb_index_t *idx, size_t count) {
  (void )(db);
  rydb_hashtable_header_t *header = (void *)idx->index.file.start;
  asserteq(header->bucket.count.used, count);
  size_t levels_total = header->bucket.bitlevel.top.count;
  for(int i = 0; i<header->bucket.count.sub_bitlevels; i++) {
    levels_total += header->bucket.bitlevel.sub[i].count;
  }
  asserteq(levels_total, count);
}
char *argv_extract2(int *argc, char **argv, off_t i) {
  char *ret = argv[i+1];
  for(off_t n=i; n<*argc-2; n++) {
    //printf("argv[%i] \"%s\" argv[%i] \"%s\"\n", i, argv[i], i+2, argv[i+2]);
    argv[n]=argv[n+2];
  }
  *argc-=2;
  return ret;
}

void data_fill(char *buf, int len, int i) {
  char chunk[32];
  sprintf(chunk, "%i ", i);
  char *end = &buf[len];
  char *cur;
  size_t clen = strlen(chunk);
  for(cur = buf; &cur[clen] < &buf[len]; cur+=clen) {
    memcpy(cur, chunk, clen);
  }
  if(end - cur > 0) {
    memcpy(cur, chunk, end - cur);
  }
  end[0]='\00';
}

const uint8_t vectors_siphash_2_4_64[64][8] = {
  {0x31, 0x0e, 0x0e, 0xdd, 0x47, 0xdb, 0x6f, 0x72,}, {0xfd, 0x67, 0xdc, 0x93, 0xc5, 0x39, 0xf8, 0x74,},
  {0x5a, 0x4f, 0xa9, 0xd9, 0x09, 0x80, 0x6c, 0x0d,}, {0x2d, 0x7e, 0xfb, 0xd7, 0x96, 0x66, 0x67, 0x85,},
  {0xb7, 0x87, 0x71, 0x27, 0xe0, 0x94, 0x27, 0xcf,}, {0x8d, 0xa6, 0x99, 0xcd, 0x64, 0x55, 0x76, 0x18,},
  {0xce, 0xe3, 0xfe, 0x58, 0x6e, 0x46, 0xc9, 0xcb,}, {0x37, 0xd1, 0x01, 0x8b, 0xf5, 0x00, 0x02, 0xab,},
  {0x62, 0x24, 0x93, 0x9a, 0x79, 0xf5, 0xf5, 0x93,}, {0xb0, 0xe4, 0xa9, 0x0b, 0xdf, 0x82, 0x00, 0x9e,},
  {0xf3, 0xb9, 0xdd, 0x94, 0xc5, 0xbb, 0x5d, 0x7a,}, {0xa7, 0xad, 0x6b, 0x22, 0x46, 0x2f, 0xb3, 0xf4,},
  {0xfb, 0xe5, 0x0e, 0x86, 0xbc, 0x8f, 0x1e, 0x75,}, {0x90, 0x3d, 0x84, 0xc0, 0x27, 0x56, 0xea, 0x14,},
  {0xee, 0xf2, 0x7a, 0x8e, 0x90, 0xca, 0x23, 0xf7,}, {0xe5, 0x45, 0xbe, 0x49, 0x61, 0xca, 0x29, 0xa1,},
  {0xdb, 0x9b, 0xc2, 0x57, 0x7f, 0xcc, 0x2a, 0x3f,}, {0x94, 0x47, 0xbe, 0x2c, 0xf5, 0xe9, 0x9a, 0x69,},
  {0x9c, 0xd3, 0x8d, 0x96, 0xf0, 0xb3, 0xc1, 0x4b,}, {0xbd, 0x61, 0x79, 0xa7, 0x1d, 0xc9, 0x6d, 0xbb,},
  {0x98, 0xee, 0xa2, 0x1a, 0xf2, 0x5c, 0xd6, 0xbe,}, {0xc7, 0x67, 0x3b, 0x2e, 0xb0, 0xcb, 0xf2, 0xd0,},
  {0x88, 0x3e, 0xa3, 0xe3, 0x95, 0x67, 0x53, 0x93,}, {0xc8, 0xce, 0x5c, 0xcd, 0x8c, 0x03, 0x0c, 0xa8,},
  {0x94, 0xaf, 0x49, 0xf6, 0xc6, 0x50, 0xad, 0xb8,}, {0xea, 0xb8, 0x85, 0x8a, 0xde, 0x92, 0xe1, 0xbc,},
  {0xf3, 0x15, 0xbb, 0x5b, 0xb8, 0x35, 0xd8, 0x17,}, {0xad, 0xcf, 0x6b, 0x07, 0x63, 0x61, 0x2e, 0x2f,},
  {0xa5, 0xc9, 0x1d, 0xa7, 0xac, 0xaa, 0x4d, 0xde,}, {0x71, 0x65, 0x95, 0x87, 0x66, 0x50, 0xa2, 0xa6,},
  {0x28, 0xef, 0x49, 0x5c, 0x53, 0xa3, 0x87, 0xad,}, {0x42, 0xc3, 0x41, 0xd8, 0xfa, 0x92, 0xd8, 0x32,},
  {0xce, 0x7c, 0xf2, 0x72, 0x2f, 0x51, 0x27, 0x71,}, {0xe3, 0x78, 0x59, 0xf9, 0x46, 0x23, 0xf3, 0xa7,},
  {0x38, 0x12, 0x05, 0xbb, 0x1a, 0xb0, 0xe0, 0x12,}, {0xae, 0x97, 0xa1, 0x0f, 0xd4, 0x34, 0xe0, 0x15,},
  {0xb4, 0xa3, 0x15, 0x08, 0xbe, 0xff, 0x4d, 0x31,}, {0x81, 0x39, 0x62, 0x29, 0xf0, 0x90, 0x79, 0x02,},
  {0x4d, 0x0c, 0xf4, 0x9e, 0xe5, 0xd4, 0xdc, 0xca,}, {0x5c, 0x73, 0x33, 0x6a, 0x76, 0xd8, 0xbf, 0x9a,},
  {0xd0, 0xa7, 0x04, 0x53, 0x6b, 0xa9, 0x3e, 0x0e,}, {0x92, 0x59, 0x58, 0xfc, 0xd6, 0x42, 0x0c, 0xad,},
  {0xa9, 0x15, 0xc2, 0x9b, 0xc8, 0x06, 0x73, 0x18,}, {0x95, 0x2b, 0x79, 0xf3, 0xbc, 0x0a, 0xa6, 0xd4,},
  {0xf2, 0x1d, 0xf2, 0xe4, 0x1d, 0x45, 0x35, 0xf9,}, {0x87, 0x57, 0x75, 0x19, 0x04, 0x8f, 0x53, 0xa9,},
  {0x10, 0xa5, 0x6c, 0xf5, 0xdf, 0xcd, 0x9a, 0xdb,}, {0xeb, 0x75, 0x09, 0x5c, 0xcd, 0x98, 0x6c, 0xd0,},
  {0x51, 0xa9, 0xcb, 0x9e, 0xcb, 0xa3, 0x12, 0xe6,}, {0x96, 0xaf, 0xad, 0xfc, 0x2c, 0xe6, 0x66, 0xc7,},
  {0x72, 0xfe, 0x52, 0x97, 0x5a, 0x43, 0x64, 0xee,}, {0x5a, 0x16, 0x45, 0xb2, 0x76, 0xd5, 0x92, 0xa1,},
  {0xb2, 0x74, 0xcb, 0x8e, 0xbf, 0x87, 0x87, 0x0a,}, {0x6f, 0x9b, 0xb4, 0x20, 0x3d, 0xe7, 0xb3, 0x81,},
  {0xea, 0xec, 0xb2, 0xa3, 0x0b, 0x22, 0xa8, 0x7f,}, {0x99, 0x24, 0xa4, 0x3c, 0xc1, 0x31, 0x57, 0x24,},
  {0xbd, 0x83, 0x8d, 0x3a, 0xaf, 0xbf, 0x8d, 0xb7,}, {0x0b, 0x1a, 0x2a, 0x32, 0x65, 0xd5, 0x1a, 0xea,},
  {0x13, 0x50, 0x79, 0xa3, 0x23, 0x1c, 0xe6, 0x60,}, {0x93, 0x2b, 0x28, 0x46, 0xe4, 0xd7, 0x06, 0x66,},
  {0xe1, 0x91, 0x5f, 0x5c, 0xb1, 0xec, 0xa4, 0x6c,}, {0xf3, 0x25, 0x96, 0x5c, 0xa1, 0x6d, 0x62, 0x9f,},
  {0x57, 0x5f, 0xf2, 0x8e, 0x60, 0x38, 0x1b, 0xe5,}, {0x72, 0x45, 0x06, 0xeb, 0x4c, 0x32, 0x8a, 0x95,},
};

const struct vector_crc32_s vector_crc32[32] = {
  {"", 0x0},
  {"foo", 0x8c736521},
  {"bar", 0x76ff8caa},
  {"longer", 0xe5765c79},
  {"this is a longer string", 0xfef3918f},
  {"yep another one", 0xbe9ffb21},
  {"okay that's enough for the time being", 0x9b6a0b25},
  {"0", 0xf4dbdf21},
  {"1", 0x83dcefb7},
  {"2", 0x1ad5be0d},
  {"3", 0x6dd28e9b},
  {"4", 0xf3b61b38},
  {"5", 0x84b12bae},
  {"6", 0x1db87a14},
  {"10", 0xa15d25e1},
  {"1000", 0xb427a317},
  {"33", 0xa6216d9},
  {"44", 0xdb4715bd},
  {"qt3.14159", 0xb673bc02},
  {"1000000", 0x1bfcd5dd},
  {"100000000000000000000000000", 0xefeb0bfd},
  {NULL, 0}
};
