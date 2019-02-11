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

void test_errhandler(rydb_t *db, rydb_error_t *err, void *privdata);

void fail_malloc_after(int n);
void fail_malloc_later_each_time(void);
void reset_malloc(void);

int rmdir_recursive(const char *path);

#define assert_db_ok(db, cmd) \
  do { \
    char buf[1024]; \
    int cmd_rc = cmd;\
    if(rydb_error(db)) \
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
    else if(err->code != expected_error) \
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

const uint8_t vectors_siphash_2_4_64[64][8];


#endif // __TEST_UTIL_H
