#ifndef __TEST_UTIL_H
#define __TEST_UTIL_H

#include "rydb_internal.h"
#include "snow.h"
#include <signal.h>
#include <regex.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>

#undef snow_main_decls
#define snow_main_decls \
	void snow_break() {} \
	void snow_rerun_failed() {raise(SIGSTOP);} \
	struct _snow _snow; \
	int _snow_inited = 0

#define skip(name) while(0) 

int is_little_endian(void);

#define ROW_LEN 20
void config_testdb(rydb_t *db);

void test_errhandler(rydb_t *db, rydb_error_t *err, void *privdata);

void fail_malloc_after(int n);
void fail_malloc_later_each_time(void);
void reset_malloc(void);

int rmdir_recursive(const char *path);
void rydb_print_stored_data(rydb_t *db);

#if RYDB_DEBUG
char intercepted_printf_buf[4096];
int rydb_intercept_printfs(void);
int rydb_unintercept_printfs(void);
#endif

int rydb_reopen(rydb_t **db);

int ___rydb_failed_as_expected(rydb_t *db, char *callstr, int rc, rydb_error_code_t expected_error_code, const char *errmsg_match, char *errmsg_result);

int sed_meta_file(rydb_t *db, char *regex);
int sed_meta_file_prop(rydb_t *db, const char *prop, const char *val);

struct cmd_rownum_out_of_range_check_s {
  char      *name;
  rydb_row_t rows[2];
  int        n;
  int        n_check;
};
void cmd_rownum_out_of_range_check(rydb_t *db, struct cmd_rownum_out_of_range_check_s *check, int nrows);


#define assert_ptr_aligned(ptr, alignment) \
do { \
  if((uintptr_t )ptr % alignment != 0) { \
    fail("" #ptr " fails %i-byte-alignment by %i bytes", (int )alignment, (int)((uintptr_t )ptr % alignment)); \
  } \
} while(0)

#define MAXERRLEN 2048

#define assert_db_ok(db, cmd) \
  do { \
    char ___buf[MAXERRLEN]; \
    int ___rc = (cmd);\
    rydb_error_snprint(db, ___buf, MAXERRLEN); \
    if(___rc != 1) \
      fail("%s", ___buf); \
  } while(0)

#define assert_db_fail(db, cmd, expected_error, ...) \
  do { \
    snow_fail_update(); \
    char ___buf[MAXERRLEN]; \
    char *___rxpattern = "" __VA_ARGS__; \
    int ___rc = (cmd); \
    if(___rydb_failed_as_expected(db, #cmd, ___rc, expected_error, ___rxpattern, ___buf) == 0 ) { \
      rydb_error_clear(db); \
      snow_fail("%s", ___buf); \
    } \
  } while(0)
  
#define assert_db_fail_match_errstr(db, cmd, expected_error, match) \
  do { \
    snow_fail_update(); \
    char ___buf[MAXERRLEN]; \
    int ___rc = (cmd); \
    if(___rydb_failed_as_expected(db, #cmd, ___rc, expected_error, match,  ___buf) == 0 ) { \
      rydb_error_clear(db); \
      snow_fail("%s", ___buf); \
    } \
  } while(0)

#define assert_db(db) \
  do { \
    char ___cmd_rc[MAXERRLEN]; \
    assertneq(db, NULL, "rydb struct pointer cannot be NULL"); \
    rydb_error_t *err = rydb_error(db); \
    if(err) { \
      rydb_error_snprint(db, ___cmd_rc, MAXERRLEN); \
    } \
    if(err) \
      fail("Expected no error, got %s", ___cmd_rc); \
  } while(0)

#define assert_db_row_type(db, row, rowtype) \
  do { \
    if(row->type != rowtype) { \
      fail("(rydb_row_type_t) Expected " #row " (rownum %i) to be " #rowtype ", but got %s", (int )rydb_row_to_rownum(db, row), rydb_rowtype_str(row->type)); \
    } \
  } while(0)
  
#define assert_db_row_type_not(db, row, rowtype) \
  do { \
    if(row->type == rowtype) { \
      fail("(rydb_row_type_t) Expected " #row " (rownum %i) to NOT be " #rowtype ", but got %s", (int )rydb_row_to_rownum(db, row), rydb_rowtype_str(row->type)); \
    } \
  } while(0)

#define assert_db_row_target_rownum(db, row, trownum) \
  do { \
    rydb_rownum_t ___expected_rownum = trownum; \
    if(row->target_rownum != ___expected_rownum) { \
      fail("(rydb_row_type_t) Expected " #row " (rownum %i) target_rownum to be %i, but got %i", (int )rydb_row_to_rownum(db, row), ___expected_rownum, (int )row->target_rownum); \
    } \
  } while(0)
  
#define assert_db_row_data(db, row, compare_data) \
  do { \
    char ___cmp[1024]; \
    memset(___cmp, '\00', db->config.row_len+1); \
    strncpy(___cmp, compare_data, db->config.row_len); \
    int ___rc = strcmp(___cmp, row->data); \
    if(___rc != 0) { \
      fail("(rydb_stored_row_t) " #row " (rownum %i) data does not match. expected \"%s\", got \"%s\".", (int )rydb_row_to_rownum(db, row), ___cmp, row->data); \
    } \
  } while(0)

#define assert_db_datarow(db, row, compare_data) \
  do { \
    if(compare_data) { \
      assert_db_row_type(db, row, RYDB_ROW_DATA); \
      assert_db_row_data(db, row, compare_data); \
    } \
    else { \
      assert_db_row_type(db, row, RYDB_ROW_EMPTY); \
    } \
  } while(0)

#define assert_data_match(db, rows, n) \
  do { \
    int ___n = 0; \
    RYDB_EACH_ROW(db, cur) { \
      if(___n < n) { \
        assert_db_datarow(db, cur, rows[___n]); \
      } \
      else { \
        assert_db_row_type_not(db, cur, RYDB_ROW_DATA); \
      } \
      ___n++; \
    } \
  } while(0)
  
#define assert_data_rownum_match(db, n, match) \
  do { \
    int ___rownum = n; \
    rydb_stored_row_t *___row = rydb_rownum_to_row(db, ___rownum); \
    if(!___row) { \
      fail("Row %i does not exist", ___rownum); \
    } \
    assert_db_datarow(db, ___row, match); \
  } while(0)
  
  #define assert_data_rownum_type(db, n, match_type) \
  do { \
    int ___rownum = n; \
    rydb_stored_row_t *___row = rydb_rownum_to_row(db, ___rownum); \
    if(!___row) { \
      fail("Row %i does not exist", ___rownum); \
    } \
    assert_db_row_type(db, ___row, match_type); \
  } while(0)
  
#define assert_db_insert_rows(db, rows, nrows) \
  do { \
    int ___maxrows = nrows; \
    char **___rowdata = rows; \
    for(int ___i=0; ___i<___maxrows; ___i++) { \
      assert_db_ok(db, rydb_row_insert_str(db, ___rowdata[___i])); \
    } \
  } while(0);
  
  
  
const uint8_t vectors_siphash_2_4_64[64][8];
struct vector_crc32_s {
  const char *in;
  uint64_t out;
};
const struct vector_crc32_s vector_crc32[32];

off_t filesize(const char *filename);

#endif // __TEST_UTIL_H
