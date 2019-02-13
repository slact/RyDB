#ifndef __TEST_UTIL_H
#define __TEST_UTIL_H

#include "rydb_internal.h"
#include "snow.h"
#include <signal.h>

#undef snow_main_decls
#define snow_main_decls \
	void snow_break() {} \
	void snow_rerun_failed() {raise(SIGSTOP);} \
	struct _snow _snow; \
	int _snow_inited = 0

#define skip(name) while(0) 
  
void test_errhandler(rydb_t *db, rydb_error_t *err, void *privdata);

void fail_malloc_after(int n);
void fail_malloc_later_each_time(void);
void reset_malloc(void);

int rmdir_recursive(const char *path);
void rydb_print_stored_data(rydb_t *db);

#define assert_db_ok(db, cmd) \
  do { \
    char buf[1024]; \
    int cmd_rc = cmd;\
    rydb_error_snprint(db, buf, 1024); \
    if(cmd_rc != 1) \
      fail("%s", buf); \
  } while(0)

#define assert_db_fail(db, cmd, expected_error) \
  do { \
    int cmd_rc = cmd;\
    char buf[1024]; \
    rydb_error_t *err = rydb_error(db); \
    if(err) \
      rydb_error_snprint(db, buf, 1024); \
    if(cmd_rc != 0) \
      fail("Expected to fail with error %s [%i], but succeeded instead", rydb_error_code_str(expected_error), expected_error); \
    else if(err && err->code != expected_error) \
      fail("Expected to fail with error %s [%i], but got %s", rydb_error_code_str(expected_error), expected_error, buf); \
    rydb_error_clear(db); \
  } while(0)

#define assert_db(db) \
  do { \
    char buf[1024]; \
    assertneq(db, NULL, "rydb struct pointer cannot be NULL"); \
    rydb_error_t *err = rydb_error(db); \
    if(err) { \
      rydb_error_snprint(db, buf, 1024); \
    } \
    if(err) \
      fail("Expected no error, got %s", buf); \
  } while(0)

#define assert_db_row_type(db, row, rowtype) \
  do { \
    if(row->type != rowtype) { \
      fail("(rydb_row_type_t) Expected " #row " (rownum %i) to be " #rowtype ", but got %s", (int )rydb_row_to_rownum(db, row), rydb_rowtype_str(row->type)); \
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
    char *___cmp = malloc(db->config.row_len+1); \
    if(!___cmp) { \
      fail("assert_db_row_data because malloc() failed"); \
    } \
    memset(___cmp, '\00', db->config.row_len+1); \
    strncpy(___cmp, compare_data, db->config.row_len); \
    int ___rc = strcmp(___cmp, row->data); \
    free(___cmp); \
    if(___rc != 0) { \
      fail("(rydb_stored_row_t) " #row " (rownum %i) data does not match.", (int )rydb_row_to_rownum(db, row)); \
    } \
  } while(0)

#define assert_db_datarow(db, row, compare_data, n) \
  do { \
    assert_db_row_type(db, row, RYDB_ROW_DATA); \
    assert_db_row_target_rownum(db, row, 0); \
    assert_db_row_data(db, row, compare_data[n]); \
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


#endif // __TEST_UTIL_H
