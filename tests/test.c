#include <rydb_internal.h>
#include <rydb_hashtable.h>
#include "test_util.h"

void test_errhandler(rydb_t *db, rydb_error_t *err, void *privdata) {
  asserteq(db, privdata);
  asserteq(err->code, RYDB_ERROR_BAD_CONFIG);
}
#define ROW_LEN 20
static void config_testdb(rydb_t *db) {
  assert_db_ok(db, rydb_config_row(db, 20, 5));
  assert_db_ok(db, rydb_config_add_index_hashtable(db, "foo", 5, 5, RYDB_INDEX_DEFAULT, NULL));
  assert_db_ok(db, rydb_config_add_index_hashtable(db, "bar", 10, 5, RYDB_INDEX_DEFAULT, NULL));
  assert_db_ok(db, rydb_config_add_row_link(db, "next", "prev"));
  assert_db_ok(db, rydb_config_add_row_link(db, "fwd", "rew"));
}

describe(struct_size) {
  test("rydb_row_cmd_header_t is unpadded") {
    asserteq(sizeof(rydb_row_cmd_header_t), sizeof(uint16_t)*2);
  }
  test("rydb_stored_row_t data offset is unpadded") {
    asserteq(offsetof(rydb_stored_row_t, data), sizeof(uint8_t)*2 + sizeof(rydb_rownum_t));
  }
}

describe(hashing) {
  subdesc(siphash_2_4_64bit) {
    it("has the expected output") {
      uint8_t in[64], k[16], out[8];
      
      //initialize key
      for (int i = 0; i < 16; ++i) k[i] = i;
      
      for (int i = 0; i < 64; ++i) {
        in[i] = i;
        *(uint64_t *)out = siphash(in, i, k);
        if(memcmp(out, vectors_siphash_2_4_64[i], 8)!=0) {
          fail("mismatch at test vector %i", i);
        }
      }
    }
  }
}

describe(rydb_new) {
  it("fails gracefully when out of memory") {
    fail_malloc_after(0);
    asserteq(rydb_new(), NULL);
    reset_malloc();
  }
  it("gets initialized as one might expect") {
    rydb_t *db = rydb_new();
    assertneq(db, NULL);
    rydb_close(db);
  }
}
describe(config) {
  rydb_t *db;
  char path[64];
  
  before_each() {
    reset_malloc();
    db = rydb_new();
    assert_db(db);
    strcpy(path, "test.db.insert_rows.XXXXXX");
    mkdtemp(path);
  }
  after_each() {
    rydb_close(db);
    db = NULL;
    rmdir_recursive(path);
  }
  
  subdesc(row)   {
    it("fails on bad length params") {
      assert_db_fail(db, rydb_config_row(db, 10, 20), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_row(db, 0, 0), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_row(db, RYDB_ROW_LEN_MAX+1, 0), RYDB_ERROR_BAD_CONFIG);
      asserteq(db->config.row_len, 0);
      asserteq(db->config.id_len, 0);
    }
    it("sets lengths right") {
      assert_db_ok(db, rydb_config_row(db, 10, 4));
      asserteq(db->config.row_len, 10);
      asserteq(db->config.id_len, 4);
    }
    
    it("fails if db is already open") {
      config_testdb(db);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_row(db, 10, 4), RYDB_ERROR_DATABASE_OPEN);
    }
  }
  
  subdesc(row_link) {
    it("fails on weird link names") {
      assert_db_fail(db, rydb_config_add_row_link(db, "", "meh"), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", ""), RYDB_ERROR_BAD_CONFIG);
      
      char bigname[RYDB_NAME_MAX_LEN+10];
      memset(bigname, 'z', sizeof(bigname));
      bigname[sizeof(bigname)-1]='\00';
      assert_db_fail(db, rydb_config_add_row_link(db, bigname, "meh"), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", bigname), RYDB_ERROR_BAD_CONFIG);
      
      assert_db_fail(db, rydb_config_add_row_link(db, "non-alphanum!", "meh"), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", "non-alphanum!"), RYDB_ERROR_BAD_CONFIG);
      
      assert_db_fail(db, rydb_config_add_row_link(db, "same", "same"), RYDB_ERROR_BAD_CONFIG);
    }
    it("fails gracefully when out of memory") {

      fail_malloc_later_each_time();
      assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
      asserteq(db->config.link_pair_count, 0);
      assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
      asserteq(db->config.link_pair_count, 0);
      assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
      asserteq(db->config.link_pair_count, 0);
      fail_malloc_after(2);
      assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
      asserteq(db->config.link_pair_count, 0);
      reset_malloc();
      
      fail_malloc_after(6);
      assert_db_ok(db, rydb_config_add_row_link(db, "foo", "bar"));
      asserteq(db->config.link_pair_count, 1);
      assert_db_ok(db, rydb_config_add_row_link(db, "foo2", "bar2"));
      asserteq(db->config.link_pair_count, 2);
      assert_db_fail(db, rydb_config_add_row_link(db, "foo3", "bar3"), RYDB_ERROR_NOMEMORY);
      asserteq(db->config.link_pair_count, 2);
      assertneq(db->config.link, NULL);
      reset_malloc();
    }
    
    it("fails on repeated row link names") {
      assert_db_ok(db, rydb_config_add_row_link(db, "next", "prev"));
      assert_db_fail(db, rydb_config_add_row_link(db, "next", "meh"), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", "next"), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_row_link(db, "prev", "meh"), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", "prev"), RYDB_ERROR_BAD_CONFIG);
    }
    
    it("fails on too many links") {
      for(int i=0; i<RYDB_ROW_LINK_PAIRS_MAX; i++) {
        char prevname[32], nextname[32];
        sprintf(prevname, "prev%i", i);
        sprintf(nextname, "next%i", i);
        assert_db_ok(db, rydb_config_add_row_link(db, nextname, prevname));
      }
      
      //too many links
      assert_db_fail(db, rydb_config_add_row_link(db, "next1000", "prev1000"), RYDB_ERROR_BAD_CONFIG);
      
      asserteq(db->config.link_pair_count, RYDB_ROW_LINK_PAIRS_MAX);
    }
    
    it("sorts link names") {
      for(int i=0; i<RYDB_ROW_LINK_PAIRS_MAX; i++) {
        char prevname[32], nextname[32];
        sprintf(prevname, "prev%i", i);
        sprintf(nextname, "next%i", i);
        assert_db_ok(db, rydb_config_add_row_link(db, nextname, prevname));
      }
      const char *cur = NULL, *prev="\00";
      char cpy[32];
      rydb_config_row_link_t *links = db->config.link;
      for(int i=0; i<RYDB_ROW_LINK_PAIRS_MAX * 2; i++) {
        cur = links[i].next;
        strcpy(cpy, cur);
        memcpy(cpy, cur[0]=='n' ? "prev" : "next", 4);
        assert(strcmp(cur, prev)>0, "row links are supposed to be sorted");
        asserteq(links[i].prev, cpy, "that link-prev is wrong");
        assert(links[i].inverse == (cur[0]=='n' ? 0 : 1));
        
        prev = cur;
      }
    }
    it("fails if db is already open") {
      config_testdb(db);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_add_row_link(db, "next", "prev"), RYDB_ERROR_DATABASE_OPEN);
    }
  }
  
  subdesc(errors) {
    it("sets error_handler correctly") {
      rydb_set_error_handler(db, test_errhandler, db);
      assert_db_fail(db, rydb_config_row(db, 0, 0), RYDB_ERROR_BAD_CONFIG);
    }
  }
  
  subdesc(add_hashtable_index) {
    it("fails on bad flags") {
      rydb_config_row(db, 20, 5);
      
      //bad flags
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, 0xFF, NULL), RYDB_ERROR_BAD_CONFIG);
    }
    
    it("fails on bad hashtable config") {
      rydb_config_index_hashtable_t cf = {
        .hash_function = -1,
        .store_value = 1,
        .direct_mapping = 1
      };
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG);
    }
    it("fails if index name is weird") {
      char bigname[RYDB_NAME_MAX_LEN+10];
      memset(bigname, 'z', sizeof(bigname)-1);
      bigname[sizeof(bigname)-1]='\00';
      assert_db_fail(db, rydb_config_add_index_hashtable(db, bigname, 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
      
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "shouldn't have spaces", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
    }
    it("fails if index start or length are out-of-bounds") {
      rydb_config_row(db, 20, 5);
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 0, 30, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 30, 1, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
    }
    it("fails on duplicate index name") {
      rydb_config_row(db, 20, 5);
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL));
      //can't add duplicate index
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
    }
    it("ensures the primary index is unique") {
      rydb_config_row(db, 20, 5);
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
    }
     
    it("fails if adding too many indices") {
      rydb_config_row(db, 20, 5);
      //add all the possible indices
      for(int i=0; i < RYDB_INDICES_MAX-1; i++) {
        char indexname[32];
        sprintf(indexname, "index%i", i);
        assert_db_ok(db, rydb_config_add_index_hashtable(db, indexname, 5, 5, RYDB_INDEX_DEFAULT, NULL));
      }
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_UNIQUE, NULL));
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar2", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
    }
    
    it("sorts the idices") {
      rydb_config_row(db, 20, 5);
      //add all the possible indices
      for(int i=0; i < RYDB_INDICES_MAX-1; i++) {
        char indexname[32];
        sprintf(indexname, "index%i", i);
        assert_db_ok(db, rydb_config_add_index_hashtable(db, indexname, 5, 5, RYDB_INDEX_DEFAULT, NULL));
      }
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_UNIQUE, NULL));
      //check out the indices
      asserteq(db->config.index_count, RYDB_INDICES_MAX);
      rydb_config_index_t *cur, *prev = NULL;
      for(int i=0; i<RYDB_INDICES_MAX; i++) {
        cur = &db->config.index[i];
        if(prev) {
          assert(strcmp(cur->name, prev->name) > 0, "indices are supposed to be sorted");
        }
        prev = cur;
      }
    }
    
    it("fails gracefully when out of memory") {
      rydb_config_row(db, 20, 5);
      fail_malloc_later_each_time();
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_NOMEMORY);
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_NOMEMORY);
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL));
      reset_malloc();
    }
    
    it("fails if db is already open") {
      config_testdb(db);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_DATABASE_OPEN);
    }
  }
  
  subdesc(revision) {
    it("sets db revision") {
      assert_db_ok(db, rydb_config_revision(db, 15));
      asserteq(db->config.revision, 15);
    }
    it("fails if db revision is too large") {
      assert_db_fail(db, rydb_config_revision(db, RYDB_REVISION_MAX + 1), RYDB_ERROR_BAD_CONFIG);
      asserteq(db->config.revision, 0);
    }
    it("fails if db is already open") {
      config_testdb(db);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_revision(db, RYDB_REVISION_MAX + 1), RYDB_ERROR_DATABASE_OPEN);
    }
  }
}

describe(rydb_open) {
  rydb_t *db;
  char path[64];
  
  before_each() {
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
  }
  after_each() {
    rydb_close(db);
    rmdir_recursive(path);
  }
  
  it ("gracefully fails when out of memory") {
    
    rydb_config_row(db, 20, 5);
    
    assert_db_fail(db, rydb_open(db, "./fakepath", "open_test"), RYDB_ERROR_FILE_ACCESS);
    
    fail_malloc_later_each_time();
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_ok(db, rydb_open(db, path, "open_test"));
    reset_malloc();
    
  }
  
  it("writes and reopens metadata") {
    config_testdb(db);
    assert_db_ok(db, rydb_open(db, path, "open_test"));
    rydb_close(db);
    
    db = rydb_new();
    config_testdb(db);
    assert_db_ok(db, rydb_config_add_row_link(db, "front", "back"));
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_CONFIG_MISMATCH);
    rydb_close(db);
    
    //open ok
    db = rydb_new();
    config_testdb(db);
    assert_db_ok(db, rydb_open(db, path, "open_test"));
  }
  
  it("fails to do stuff when not open") {
    config_testdb(db);
    assert_db_fail(db, rydb_row_insert_str(db, "hello"), RYDB_ERROR_DATABASE_CLOSED);
    
  }
}

describe(row_operations) {
  rydb_t *db = NULL;
  char path[64];
  
  char *rowdata[] = {
    "1.hello this is not terribly long of a string",
    "2.and this is another one that exceeds the length",
    "3.this one's short",
    "4.tiny",
    "5.here's another one",
    "6.zzzzzzzzzzzzzz"
  };
  int nrows = sizeof(rowdata)/sizeof(char *);
  
  before_each() {
    asserteq(db, NULL, "previous test not closed out correctly");
    db = rydb_new();
    strcpy(path, "test.db.insert_rows.XXXXXX");
    mkdtemp(path);
    config_testdb(db);
    assert_db_ok(db, rydb_open(db, path, "open_test"));
  }
  after_each() {
    rydb_close(db);
    db = NULL;
    rmdir_recursive(path);
  }
  
  subdesc(insert) {  
    it("inserts rows in a new database") {
      assert_db_insert_rows(db, rowdata, nrows-2);
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db);
      assert_db_ok(db, rydb_open(db, path, "open_test"));
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n < nrows-2)
          assert_db_datarow(db, cur, rowdata, n);
        else
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        n++;
      }
      assert(n - 1 == nrows-2);
      
      //now insert the remainder
      assert_db_insert_rows(db, &rowdata[nrows-2], 2);
      
      n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n < nrows) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
      assert(n - 1 == nrows);
    }
    
    it("inserts in a transaction") {
      assert_db_ok(db, rydb_transaction_start(db));
      assert_db_insert_rows(db, rowdata, nrows);
      
      //rydb_print_stored_data(db);
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        assert_db_row_type(db, cur, RYDB_ROW_CMD_SET);
        assert_db_row_target_rownum(db, cur, n+1);
        assert_db_row_data(db, cur, rowdata[n]);
        n++;
      }

      assert_db_ok(db, rydb_transaction_finish(db));
      n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n < nrows) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
      asserteq(n, nrows+1);
    }
  }
  
  subdesc(delete) {
    it("fails to delete out-of-range data rows") {
      assert_db_fail(db, rydb_row_delete(db, 0), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      assert_db_fail(db, rydb_row_delete(db, 1), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      assert_db_fail(db, rydb_row_delete(db, 100), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_fail(db, rydb_row_delete(db, nrows+1), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
    }
    
    it("deletes rows from data end") {
      assert_db_insert_rows(db, rowdata, nrows);
      //rydb_print_stored_data(db);
      for(int i=nrows; i>0; i--) {
        assert_db_ok(db, rydb_row_delete(db, i));
        assert_db_fail(db, rydb_rownum_in_data_range(db, i), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
        asserteq(rydb_row_to_rownum(db, db->data_next_row)-1, i-1);
        asserteq(rydb_row_to_rownum(db, db->cmd_next_row)-1, i-1);
        rydb_stored_row_t *row = rydb_rownum_to_row(db, i);
        assertneq(row, NULL);
        assert_db_row_type(db, row, RYDB_ROW_EMPTY);
      }
      RYDB_EACH_ROW(db, cur) {
        assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
      }
      //rydb_print_stored_data(db);
    }
    
    it("handles a range of empty rows before the last row") {
      assert_db_insert_rows(db, rowdata, nrows);
      
      //set a couple of rows before the last row as empty
      for(int i=1; i<3; i++) {
        rydb_stored_row_t *row = rydb_rownum_to_row(db, nrows - i);
        row->type = RYDB_ROW_EMPTY;
      }
      rydb_stored_row_t *row = rydb_rownum_to_row(db, 1);
      row->type = RYDB_ROW_EMPTY;
      
      //now delete the last row, and see if data_next_row is updated correctly
      assert_db_ok(db, rydb_row_delete(db, nrows));
      asserteq(rydb_row_to_rownum(db, db->data_next_row), nrows-2);
      asserteq(rydb_row_to_rownum(db, db->cmd_next_row), nrows-2);
      //rydb_print_stored_data(db);
      
    }
    
    it("deletes rows from data start") {
      assert_db_insert_rows(db, rowdata, nrows);
      
      for(int i=1; i<=nrows; i++) {
        //rydb_print_stored_data(db);
        assert_db_ok(db, rydb_row_delete(db, i));
        rydb_stored_row_t *row = rydb_rownum_to_row(db, i);
        assertneq(row, NULL);
        assert_db_row_type(db, row, RYDB_ROW_EMPTY);
        if(i < nrows) {
          // double-deletion is acceptable on all but the last data row,
          // since we don't track holes in data (YET)
          assert_db_ok(db, rydb_row_delete(db, i));
          assert_db_ok(db, rydb_rownum_in_data_range(db, i));
          asserteq(rydb_row_to_rownum(db, db->data_next_row), nrows+1);
          asserteq(rydb_row_to_rownum(db, db->cmd_next_row), nrows+1);
        }
        else {
          asserteq(rydb_row_to_rownum(db, db->data_next_row), 1);
          asserteq(rydb_row_to_rownum(db, db->cmd_next_row), 1);
        }
      }
      //rydb_print_stored_data(db);
    }
    
    it("deletes last row repeatedly in a transaction") {
      assert_db_insert_rows(db, rowdata, nrows);
      
      assert_db_ok(db, rydb_transaction_start(db));
      assert_db_ok(db, rydb_row_delete(db, nrows));
      assert_db_ok(db, rydb_row_delete(db, nrows));
      assert_db_ok(db, rydb_row_delete(db, nrows));
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n < nrows) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_CMD_DELETE);
          assert_db_row_target_rownum(db, cur, nrows);
        }
        n++;
      }
      asserteq(n, nrows + 3);
      assert_db_ok(db, rydb_transaction_finish(db));
      
      n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n < nrows - 1) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
      asserteq(n, nrows + 4);
      
      //and it works fine afterwards
      assert_db_ok(db, rydb_row_insert_str(db, "after"));
      //rydb_print_stored_data(db);
      n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n < nrows - 1) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else if(n < nrows) {
          assert_db_datarow(db, cur, &"after", 0);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
    }
  }
  
  subdesc(swap) {
    it("fails on out-of-range swaps") {
      assert_db_fail(db, rydb_row_swap(db, 0, 3), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_fail(db, rydb_row_swap(db, 1, nrows+1), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      assert_db_fail(db, rydb_row_swap(db, nrows+1, 1), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
    }
    it("swaps two rows close to the middle") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_swap(db, 2, nrows-2));
      int n = 0;
      
      RYDB_EACH_ROW(db, cur) {
        if(n+1 == 2) { //one of the swapped rows
          assert_db_datarow(db, cur, rowdata, (nrows-1) - 2);
        }
        else if(n+1 == nrows - 2) { // the other swapped row
          assert_db_datarow(db, cur, rowdata, 1);
        }
        else if(n < nrows) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
    }
    
    it("swaps all the rows one by one") {
      assert_db_insert_rows(db, rowdata, nrows);
      //this should move the first row all the way down
      for(int i = 2; i <= nrows; i++) {
        assert_db_ok(db, rydb_row_swap(db, i, i-1));
      }
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n+1 < nrows) {
          assert_db_datarow(db, cur, rowdata, n+1);
        }
        else if (n+1 == nrows) {
          assert_db_datarow(db, cur, rowdata, 0);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
    }
    
    it("swaps rows with an empty row") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_delete(db, 1));
      
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        if ( n > 0 && n < nrows) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
      assert_db_ok(db, rydb_row_swap(db, 1, nrows-1));
      assert_db_ok(db, rydb_row_delete(db, nrows-2));
      assert_db_ok(db, rydb_row_swap(db, nrows, nrows-2));
      assert_db_ok(db, rydb_row_insert_str(db, "after"));
      n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n==0) {
          assert_db_datarow(db, cur, rowdata, 4);
        }
        else if(n < 3) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else if(n == 3) {
          assert_db_datarow(db, cur, rowdata, 5);
        }
        else if(n == 4) {
          assert_db_datarow(db, cur, &"after", 0);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
    }
  }
  
  subdesc(update) {
    it("fails on out-of-range updates") {
      assert_db_fail(db, rydb_row_update(db, 0, "hey", 3, 3), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      assert_db_fail(db, rydb_row_update(db, 9, "hey", 3, 3), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_fail(db, rydb_row_update(db, nrows+1, "hey", 3, 3), RYDB_ERROR_ROWNUM_OUT_OF_RANGE);
    }
    it("fails on updates that are longer than the row length") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_fail(db, rydb_row_update(db, 1, "hey", ROW_LEN, 3), RYDB_ERROR_DATA_TOO_LARGE);
      assert_db_fail(db, rydb_row_update(db, 1, "zzzzzzzzzzzzzzzzzzzzzzzzzz", 0, ROW_LEN+1), RYDB_ERROR_DATA_TOO_LARGE);
    }
    
    it("updates a small part of a row") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_update(db, nrows, "hey", 3, 3));
      //rydb_print_stored_data(db);
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n<nrows-1) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else if(n == nrows-1) {
          assert_db_datarow(db, cur, &"6.zheyzzzzzzzzzz", 0);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
    }
    
    it("updates a large part of a row") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_update(db, nrows, "heywhatis this even", 3, 17));
      assert_db_ok(db, rydb_row_update(db, nrows-1, "................................", 0, ROW_LEN));
      //rydb_print_stored_data(db);
      int n = 0;
      RYDB_EACH_ROW(db, cur) {
        if(n<nrows-2) {
          assert_db_datarow(db, cur, rowdata, n);
        }
        else if(n == nrows-2) {
          assert_db_datarow(db, cur, &"....................", 0);
        }
        else if(n == nrows-1) {
          assert_db_datarow(db, cur, &"6.zheywhatis this ev", 0);
        }
        else {
          assert_db_row_type(db, cur, RYDB_ROW_EMPTY);
        }
        n++;
      }
    }
    
  }
}

snow_main();
