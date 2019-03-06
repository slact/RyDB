#include <rydb_internal.h>
#include <rydb_hashtable.h>
#include <math.h>
#include "test_util.h"

double repeat_multiplier = 1.0;

void test_errhandler(rydb_t *db, rydb_error_t *err, void *privdata) {
  asserteq(db, privdata);
  asserteq(err->code, RYDB_ERROR_BAD_CONFIG);
}

describe(hashing) {
  test("siphash_2_4_64bit") {
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
  
  test("crc32") {
    for(unsigned i=0; vector_crc32[i].in; i++) {
      uint64_t out = crc32((uint8_t *)vector_crc32[i].in, strlen(vector_crc32[i].in));
      asserteq(out, vector_crc32[i].out);
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
  static rydb_t *db;
  static char path[64];
  
  before_each() {
    reset_malloc();
    db = rydb_new();
    assert_db(db);
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
    (void)(&db);
  }
  after_each() {
    rydb_close(db);
    db = NULL;
    rmdir_recursive(path);
  }
  
  subdesc(row)   {    
    it("fails on bad length params") {
      rydb_config_row(db, 10, 20);
      assert_db_fail(db, rydb_config_row(db, 10, 20), RYDB_ERROR_BAD_CONFIG, "cannot exceed row length");
      assert_db_fail(db, rydb_config_row(db, 0, 0), RYDB_ERROR_BAD_CONFIG, "length cannot be 0");
      assert_db_fail(db, rydb_config_row(db, RYDB_ROW_LEN_MAX+1, 0), RYDB_ERROR_BAD_CONFIG, "length [0-9]+ cannot exceed [0-9]+");
      asserteq(db->config.row_len, 0);
      asserteq(db->config.id_len, 0);
    }
    it("sets lengths right") {
      assert_db_ok(db, rydb_config_row(db, 10, 4));
      asserteq(db->config.row_len, 10);
      asserteq(db->config.id_len, 4);
    }
    
    it("fails if db is already open") {
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_row(db, 10, 4), RYDB_ERROR_DATABASE_OPEN, "open .*cannot be configured");
    }
  }
  
  subdesc(row_link) {
    it("fails on weird link names") {
      assert_db_fail(db, rydb_config_add_row_link(db, "", "meh"), RYDB_ERROR_BAD_CONFIG, "name .*length 0");
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", ""), RYDB_ERROR_BAD_CONFIG, "reverse .*name .*length 0");
      
      char bigname[RYDB_NAME_MAX_LEN+10];
      memset(bigname, 'z', sizeof(bigname));
      bigname[sizeof(bigname)-1]='\00';
      assert_db_fail(db, rydb_config_add_row_link(db, bigname, "meh"), RYDB_ERROR_BAD_CONFIG, "name is too long");
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", bigname), RYDB_ERROR_BAD_CONFIG, "[Rr]everse .*name is too long");
      
      assert_db_fail(db, rydb_config_add_row_link(db, "non-alphanum!", "meh"), RYDB_ERROR_BAD_CONFIG, "must be alphanumeric");
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", "non-alphanum!"), RYDB_ERROR_BAD_CONFIG, "[Rr]everse .*must be alphanumeric");
      
      assert_db_fail(db, rydb_config_add_row_link(db, "same", "same"), RYDB_ERROR_BAD_CONFIG, "cannot be the same");
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
      assert_db_fail(db, rydb_config_add_row_link(db, "next", "meh"), RYDB_ERROR_BAD_CONFIG, "already exists");
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", "next"), RYDB_ERROR_BAD_CONFIG, "already exists");
      assert_db_fail(db, rydb_config_add_row_link(db, "prev", "meh"), RYDB_ERROR_BAD_CONFIG, "already exists");
      assert_db_fail(db, rydb_config_add_row_link(db, "meh", "prev"), RYDB_ERROR_BAD_CONFIG, "already exists");
    }
    
    it("fails on too many links") {
      for(int i=0; i<RYDB_ROW_LINK_PAIRS_MAX; i++) {
        char prevname[32], nextname[32];
        sprintf(prevname, "prev%i", i);
        sprintf(nextname, "next%i", i);
        assert_db_ok(db, rydb_config_add_row_link(db, nextname, prevname));
      }
      
      //too many links
      assert_db_fail(db, rydb_config_add_row_link(db, "next1000", "prev1000"), RYDB_ERROR_BAD_CONFIG, "[Cc]annot exceed [0-9]+ .*link.* per database");
      
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
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_add_row_link(db, "next", "prev"), RYDB_ERROR_DATABASE_OPEN, "open .*cannot be configured");
    }
  }
  
  subdesc(errors) {
    it("sets error_handler correctly") {
      rydb_set_error_handler(db, test_errhandler, db);
      assert_db_fail(db, rydb_config_row(db, 0, 0), RYDB_ERROR_BAD_CONFIG, "[Rr]ow length cannot be 0");
    }
  }
  
  subdesc(add_hashtable_index) {
    it("fails on bad flags") {
      rydb_config_row(db, 20, 5);
      
      //bad flags
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, 0xFF, NULL), RYDB_ERROR_BAD_CONFIG, "[Uu]nknown flags");
    }
    it("fails on bad hashtable config") {
      rydb_config_row(db, 20, 5);
      rydb_config_index_hashtable_t cf = {
        .hash_function = -1,
        .store_value = 1,
        .store_hash = 1,
        .load_factor_max = 0.4,
        .rehash = RYDB_REHASH_DEFAULT,
        .collision_resolution = RYDB_OPEN_ADDRESSING
      };
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Ii]nvalid hash");
      
      cf.hash_function = RYDB_HASH_SIPHASH;
      cf.load_factor_max = 1.3;
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Ii]nvalid load_factor_max");
      
      cf.load_factor_max = 0.3;
      cf.rehash = RYDB_REHASH_ALL_AT_ONCE | RYDB_REHASH_INCREMENTAL;
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Ii]nvalid rehash flags");
      
      cf.rehash = RYDB_REHASH_ALL_AT_ONCE | RYDB_REHASH_MANUAL;
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Ii]nvalid rehash flags");
      
      cf.rehash = RYDB_REHASH_INCREMENTAL_ON_WRITE | RYDB_REHASH_MANUAL;
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Ii]nvalid rehash flags");
      
      cf.rehash = RYDB_REHASH_INCREMENTAL;
      cf.store_hash = 0;
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Rr]equires store_hash");
      
      cf.rehash = RYDB_REHASH_DEFAULT;
      cf.store_hash = 0;
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG, "[Rr]equires store_hash");
      
    }
    it("fails if index name is weird") {
      char bigname[RYDB_NAME_MAX_LEN+10];
      memset(bigname, 'z', sizeof(bigname)-1);
      bigname[sizeof(bigname)-1]='\00';
      assert_db_fail(db, rydb_config_add_index_hashtable(db, bigname, 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "[Ii]ndex name .*too long");
      
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "shouldn't have spaces", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "name .*invalid.* must consist of .*alphanumeric .*underscore");
    }
    it("fails if index start or length are out-of-bounds") {
      rydb_config_row(db, 20, 5);
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 0, 30, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "out of bounds");
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 30, 1, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "out of bounds");
    }
    it("fails on duplicate index name") {
      rydb_config_row(db, 20, 5);
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL));
      //can't add duplicate index
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "already exists");
    }
    it("fails on non-unique primary index") {
      rydb_config_row(db, 20, 5);
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "[Pp]rimary index must have .*UNIQUE.* flag");
    }
    it("fails if adding too many indices") {
      rydb_config_row(db, 20, 5);
      //add all the possible indices
      for(int i=0; i < RYDB_INDICES_MAX-1; i++) {
        char indexname[32];
        sprintf(indexname, "index%i", i);
        assert_db_ok(db, rydb_config_add_index_hashtable(db, indexname, 5, 5, RYDB_INDEX_DEFAULT, NULL));
      }
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "[Cc]annot exceed [0-9]+ indices");
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_UNIQUE, NULL));
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar2", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG, "[Cc]annot exceed [0-9]+ indices");
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
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_DATABASE_OPEN, "open and cannot be configured");
    }
    it("adds hashtable indices") {
      rydb_config_row(db, 20, 5);
      char *name = strdup("aaah"); //test that we're not using the passed-in string by reference
      assert_db_ok(db, rydb_config_add_index_hashtable(db, name, 1, 3, RYDB_INDEX_DEFAULT, NULL));
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "baah", 4, 7, RYDB_INDEX_UNIQUE, NULL));
      free(name);
      
      rydb_config_index_hashtable_t cf = {
        .hash_function = RYDB_HASH_NOHASH,
        .store_value = 1,
        .store_hash = 0,
        .collision_resolution = RYDB_OPEN_ADDRESSING,
        .load_factor_max = 0.30,
        .rehash = RYDB_REHASH_MANUAL
      };
      assert_db_ok(db, rydb_config_add_index_hashtable(db, "caah", 10, 5, RYDB_INDEX_UNIQUE, &cf));
      int i;
      for(i=0; i<db->config.index_count; i++) {
        rydb_config_index_t *cur = &db->config.index[i];
        if(i == 0) {
          asserteq(cur->name, "aaah");
          asserteq(cur->type, RYDB_INDEX_HASHTABLE);
          asserteq(cur->start, 1);
          asserteq(cur->len, 3);
          //check default configs
          asserteq(cur->type_config.hashtable.hash_function, RYDB_HASH_SIPHASH);
          
          //non-unique index defaults
          assert(cur->type_config.hashtable.store_value==0);
          assert(cur->type_config.hashtable.store_hash==1);
          assert(cur->type_config.hashtable.collision_resolution==RYDB_OPEN_ADDRESSING);
          assert(fabs(cur->type_config.hashtable.load_factor_max - RYDB_HASHTABLE_DEFAULT_MAX_LOAD_FACTOR) < 0.0001);
          assert(cur->type_config.hashtable.rehash == RYDB_HASHTABLE_DEFAULT_REHASH_FLAGS);
        }
        else if(i == 1) {
          asserteq(cur->name, "baah");
          asserteq(cur->type, RYDB_INDEX_HASHTABLE);
          asserteq(cur->start, 4);
          asserteq(cur->len, 7);
          //check default configs
          asserteq(cur->type_config.hashtable.hash_function, RYDB_HASH_SIPHASH);
          
          //unique index defaults
          assert(cur->type_config.hashtable.store_value==0);
          assert(cur->type_config.hashtable.store_hash==1);
          assert(cur->type_config.hashtable.collision_resolution==RYDB_OPEN_ADDRESSING);
          assert(fabs(cur->type_config.hashtable.load_factor_max - RYDB_HASHTABLE_DEFAULT_MAX_LOAD_FACTOR) < 0.0001);
          assert(cur->type_config.hashtable.rehash == RYDB_HASHTABLE_DEFAULT_REHASH_FLAGS);
        }
        else if(i == 2) {
          asserteq(cur->name, "caah");
          asserteq(cur->type, RYDB_INDEX_HASHTABLE);
          asserteq(cur->start, 10);
          asserteq(cur->len, 5);
          //check default configs
          asserteq(cur->type_config.hashtable.hash_function, RYDB_HASH_NOHASH);
          assert(cur->type_config.hashtable.store_value==1);
          assert(cur->type_config.hashtable.store_hash==0);
          assert(cur->type_config.hashtable.collision_resolution==RYDB_OPEN_ADDRESSING);
          assert(fabs(cur->type_config.hashtable.load_factor_max - 0.30) < 0.0001);
          assert(cur->type_config.hashtable.rehash == RYDB_REHASH_MANUAL);
        }
        else {
          fail("too many indices found");
        }
      }
      asserteq(i, 3);
    }
    it("sorts the indices") {
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
  }
  
  subdesc(revision) {
    it("fails if db revision is too large") {
      assert_db_fail(db, rydb_config_revision(db, RYDB_REVISION_MAX + 1), RYDB_ERROR_BAD_CONFIG, "[Rr]evision number cannot exceed [0-9]+");
      asserteq(db->config.revision, 0);
    }
    it("fails if db is already open") {
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "config_test"));
      assert_db_fail(db, rydb_config_revision(db, RYDB_REVISION_MAX + 1), RYDB_ERROR_DATABASE_OPEN);
    }
    it("sets db revision") {
      asserteq(db->config.revision, 0);
      assert_db_ok(db, rydb_config_revision(db, 15));
      asserteq(db->config.revision, 15);
    }
  }
}

describe(sizing) {
  static rydb_t *db;
  static char path[64];
  
  subdesc(struct_padding) {
    test("rydb_row_cmd_header_t is unpadded") {
      asserteq(sizeof(rydb_row_cmd_header_t), sizeof(uint16_t)*2);
    }
    test("rydb_stored_row_t data offset is unpadded") {
      asserteq(offsetof(rydb_stored_row_t, data), sizeof(uint8_t)*4 + sizeof(rydb_rownum_t));
    }
  }
  subdesc(alignment) {
    before_each() {
      reset_malloc();
      db = rydb_new();
      assert_db(db);
      strcpy(path, "test.db.XXXXXX");
      mkdtemp(path);
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
    }
    after_each() {
      rydb_close(db);
      db = NULL;
      rmdir_recursive(path);
    }
    test("data start is 8-byte aligned") {
      assert_ptr_aligned(offsetof(rydb_stored_row_t, data), 8);
      assert_ptr_aligned(RYDB_DATA_START_OFFSET, 8);
      assert_ptr_aligned(db->data.file.start, 8);
      asserteq((char *)db->data.data.start - (char *)db->data.file.start, RYDB_DATA_START_OFFSET);
      assert_ptr_aligned(db->data.data.start, 8);
      
      
    }
  }
}

describe(errors_and_debug) {
  static rydb_t *db = NULL;
  static char path[256];
  
  before_each() {
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
#ifdef RYDB_DEBUG
    rydb_intercept_printfs();
#endif
  }

  after_each() {
    if(db) rydb_close(db);
    db = NULL;
    rmdir_recursive(path);
#ifdef RYDB_DEBUG
    rydb_debug_hash_key = NULL;
    rydb_unintercept_printfs();
#endif
  }
  it("stringifies error codes") {
    for(int i=0; i<100; i++) {
      assertneq(rydb_error_code_str(i), NULL);
    }
  }
  
  it("stringifies row types") {
    for(int i=0; i<UINT8_MAX; i++) {
      assertneq(rydb_rowtype_str(i), NULL);
    }
  }
  
  it("clears errors on request") {
    asserteq(rydb_config_row(db, 0, 0), 0);
    rydb_error_t *err = rydb_error(db);
    assert(err);
    rydb_error_clear(db);
    err = rydb_error(db);
    asserteq(err, NULL);
  }
#if RYDB_DEBUG
  it("prints errors as one would expect") {
    asserteq(rydb_config_row(db, 0, 0), 0);
    rydb_error_t *err = rydb_error(db);
    assertneq(err, NULL);
    char errstr[1024];
    rydb_error_snprint(db, errstr, 1024);
    FILE *f = NULL;
    rydb_error_fprint(db, f);
    asserteq(intercepted_printf_buf, errstr);
    memset(intercepted_printf_buf, '\00', sizeof(intercepted_printf_buf));
    rydb_error_print(db);
    asserteq(intercepted_printf_buf, errstr);
  }
  
  test("debug-print hashtable contents") {
    rydb_debug_hash_key = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"; 
    assert_db_ok(db, rydb_config_row(db, ROW_LEN, 5));
    rydb_config_index_hashtable_t cf = {
      .rehash = RYDB_REHASH_DEFAULT, .hash_function = RYDB_HASH_SIPHASH,
      .store_value = 1, .store_hash = 1, 
      .collision_resolution = RYDB_OPEN_ADDRESSING
    };
    assert_db_ok(db, rydb_config_add_index_hashtable(db, "primary", 0, 5, RYDB_INDEX_UNIQUE, &cf));
    cf.store_value=0;
    assert_db_ok(db, rydb_config_add_index_hashtable(db, "secondary", 0, 5, RYDB_INDEX_UNIQUE, &cf));
    cf.store_hash=0;
    cf.rehash = RYDB_REHASH_ALL_AT_ONCE;
    assert_db_ok(db, rydb_config_add_index_hashtable(db, "tertiary", 0, 5, RYDB_INDEX_DEFAULT, &cf));
    assert_db_ok(db, rydb_open(db, path, "test"));
    
    char str[128];
    for(int i=0; i<2; i++) {
      sprintf(str, "%izzz", i);
      assert_db_ok(db, rydb_row_insert_str(db, str));
    }
    rydb_hashtable_print(db, &db->index[0]);
    rydb_hashtable_print(db, &db->index[1]);
    rydb_hashtable_print(db, &db->index[2]);
  }
  
  test("debug-print data") {
    assert_db_ok(db, rydb_config_row(db, ROW_LEN, 5));
    assert_db_ok(db, rydb_open(db, path, "test"));
    char str[128];
    for(int i=0; i<5; i++) {
      sprintf(str, "%izzz", i);
      assert_db_ok(db, rydb_row_insert_str(db, str));
    }
    rydb_print_stored_data(db);
    assert_db_ok(db, rydb_transaction_start(db));
    assert_db_ok(db, rydb_row_update(db, 1, "hey", 3, 3));
    assert_db_ok(db, rydb_row_update(db, 1, "123456789012345678901234567890", 0, ROW_LEN));
    rydb_print_stored_data(db);
    assert_db_ok(db, rydb_row_insert_str(db,  "beep"));
    assert_db_ok(db, rydb_row_delete(db,  3));
    assert_db_ok(db, rydb_row_swap(db, 1, 2));
    rydb_row_t rows[] = {
      {.type = RYDB_ROW_CMD_COMMIT},
      {.type = 250, .data="INVALID", .len=7, .num=0},
    };
    assert_db_ok(db, rydb_data_append_cmd_rows(db, rows, 2));
    rydb_print_stored_data(db);
  }

#endif
}

describe(rydb_open) {
  static rydb_t *db;
  static char path[64];
  
  before_each() {
    reset_malloc();
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
  }
  after_each() {
    reset_malloc();
    rydb_close(db);
    rmdir_recursive(path);
  }
  
  it("gracefully fails when out of memory") {
    rydb_config_row(db, 20, 5);
    
    assert_db_fail(db, rydb_open(db, "./fakepath", "test"), RYDB_ERROR_FILE_ACCESS, "[Ff]ailed to open file .* errno \\[2\\]");
    assert_db_ok(db, rydb_open(db, path, "open_test"));
    rydb_close(db);
    
    
    db = rydb_new();
    rydb_config_row(db, 20, 5);
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
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_fail(db, rydb_open(db, path, "open_test"), RYDB_ERROR_NOMEMORY);
    assert_db_ok(db, rydb_open(db, path, "open_test"));
    reset_malloc();
    
  }
  
  it("fails to do stuff when not open") {
    config_testdb(db, 0);
    assert_db_fail(db, rydb_row_insert_str(db, "hello"), RYDB_ERROR_DATABASE_CLOSED);
    
  }
  
  it("initializes the hash key") {
    rydb_config_row(db, 20, 5);
    assert_db_ok(db, rydb_open(db, path, "test"));
    assert(memcmp(db->config.hash_key.value, "\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00", 16) != 0);
  }
#if RYDB_DEBUG
  it("initializes the hash key without /dev/urandom") {
    rydb_debug_disable_urandom = 1;
    rydb_config_row(db, 20, 5);
    assert_db_ok(db, rydb_open(db, path, "test"));
    assert(memcmp(db->config.hash_key.value, "\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00", 16) != 0);
    assert(db->config.hash_key.quality == 0);
    rydb_debug_disable_urandom = 0;
  }
#endif
  subdesc(metadata) {
    before_each() {
      db = rydb_new();
      strcpy(path, "test.db.XXXXXX");
      mkdtemp(path);
    }
    after_each() {
      rydb_close(db);
      rmdir_recursive(path);
    }
    
    it("writes and reopens metadata") {
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db, 0);
      assert_db_ok(db, rydb_config_add_row_link(db, "front", "back"));
      assert_db_fail(db, rydb_open(db, path, "test"), RYDB_ERROR_CONFIG_MISMATCH, "[Mm]ismatch.*link.*count");
      rydb_close(db);
      
      //open ok
      db = rydb_new();
      assert_db_ok(db, rydb_open(db, path, "test"));
    }
    
    it("saves and reloads the hash key correctly") {
      char hashkey[16];
      int hashkey_quality;
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      memcpy(hashkey, db->config.hash_key.value, 16);
      hashkey_quality = db->config.hash_key.quality;
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      assert(memcmp(db->config.hash_key.value, hashkey, 16) == 0);
      assert(db->config.hash_key.quality == hashkey_quality);
      
    }
      subdesc(metadata_format_check) {
        before_each() {
          db = rydb_new();
          strcpy(path, "test.db.XXXXXX");
          mkdtemp(path);
          config_testdb(db, 0);
          assert_db_ok(db, rydb_open(db, path, "test"));
        }
        after_each() {
          rydb_close(db);
          rmdir_recursive(path);
        }
        struct metaload_test_s {
          const char *name;
          const char *val;
          rydb_error_code_t err;
          const char *match;
        };
        
        struct metaload_test_s metachecks[] = {
          {"format_revision", "1234", RYDB_ERROR_FILE_INVALID, "[Ff]ormat version mismatch"},
          {"endianness", "banana", RYDB_ERROR_FILE_INVALID, "unexpected"},
          {"endianness", is_little_endian()?"big":"little", RYDB_ERROR_WRONG_ENDIANNESS, ".*"},
          {"start_offset", "9000", RYDB_ERROR_FILE_INVALID, ".*"},
          {"type_offset", "9000", RYDB_ERROR_FILE_INVALID, "format mismatch"},
          {"reserved_offset", "9001", RYDB_ERROR_FILE_INVALID, "format mismatch"},
          {"data_offset", "9001", RYDB_ERROR_FILE_INVALID, "format mismatch"},
          {"rownum_width", "101", RYDB_ERROR_FILE_INVALID, ".*"},
          {"hash_key", "bzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzt", RYDB_ERROR_FILE_INVALID, "[Ii]nvalid hash key"},
          {"hash_key_quality", "32", RYDB_ERROR_FILE_INVALID, "[Ii]nvalid hash key quality"},
          //skip row_len check -- it's always a number, and is compared to the preconfigged value later
          {"id_len", "900000", RYDB_ERROR_BAD_CONFIG, "cannot exceed"},
          {"index_count", "1000", RYDB_ERROR_FILE_INVALID, "too many indices"},
          {"index_count", "10", RYDB_ERROR_FILE_INVALID, "[Ii]ndex specification.* invalid"},
          {"index_count", "1", RYDB_ERROR_FILE_INVALID, ".*"},
          {"name", "Non-alpha-numeric!", RYDB_ERROR_BAD_CONFIG, ".*"},
          {"type", "invalidtype", RYDB_ERROR_FILE_INVALID, "type.* invalid"},
          {"start", "9000", RYDB_ERROR_BAD_CONFIG, "out of bounds"},
          {"len", "9000", RYDB_ERROR_BAD_CONFIG, "out of bounds"},
          {"unique", "9000", RYDB_ERROR_FILE_INVALID, "invalid"},
          {"hash_function", "BananaHash", RYDB_ERROR_FILE_INVALID, ".*"},
          {"hash_function", "CRC32", RYDB_NO_ERROR, NULL},
          {"hash_function", "nohash", RYDB_NO_ERROR, NULL},
          {"hash_function", "SipHash", RYDB_NO_ERROR, NULL},
          {"load_factor_max", "10", RYDB_ERROR_FILE_INVALID, "invalid"},
          {"rehash_flags", "30", RYDB_ERROR_FILE_INVALID, "invalid"},
          {"store_value", "9000", RYDB_ERROR_FILE_INVALID, "invalid"},
          {"store_hash", "9000", RYDB_ERROR_FILE_INVALID, "invalid"},
          {"collision_resolution", "3", RYDB_ERROR_FILE_INVALID, "invalid"},
          {"link_pair_count", "9000", RYDB_ERROR_FILE_INVALID, "invalid"},
        };
        static unsigned i;
        for(i=0; i< sizeof(metachecks)/sizeof(metachecks[0]); i++) {
          char testname[128];
          struct metaload_test_s *chk = &metachecks[i];
          sprintf(testname, "fails on bad %s", chk->name);
          it(testname) {
            sed_meta_file_prop(db, chk->name, chk->val);
            if(chk->err == RYDB_NO_ERROR) {
              assert_db_ok(db, rydb_reopen(&db));
            }
            else {
              assert_db_fail_match_errstr(db, rydb_reopen(&db), chk->err, chk->match);
            }
          }
        }
        it("fails on bad index specification") {
          sed_meta_file(db, "s/ - name:/ - notname:/");
          assert_db_fail(db, rydb_reopen(&db), RYDB_ERROR_FILE_INVALID, "index.* invalid");
        }
        it("fails on missing link specification") {
          sed_meta_file(db, "s/link_pair:/banana:/");
          assert_db_fail(db, rydb_reopen(&db), RYDB_ERROR_FILE_INVALID, "link.* invalid");
        }
        it("fails on bad links") {
          sed_meta_file(db, "s/\\[ fwd , rew \\]/[bananas, morebananas]/");
          assert_db_fail(db, rydb_reopen(&db), RYDB_ERROR_FILE_INVALID, "link.* invalid");
        }
        it("fails on missing reserved_offset") {
          sed_meta_file(db, "s/reserved_offset: /foobar: /");
          assert_db_fail(db, rydb_reopen(&db), RYDB_ERROR_FILE_INVALID, "row format");
        }
        it("fails when the start of the file is wrong") {
          sed_meta_file(db, "s/--- #rydb/badfile/");
          assert_db_fail(db, rydb_reopen(&db), RYDB_ERROR_FILE_INVALID);
        }
      }
  }
}

describe(files) {
  static rydb_t *db;
  static char path[64];
  before_each() {
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
    config_testdb(db, 0);
    rydb_config_add_index_hashtable(db, "banana", 5, 4, RYDB_INDEX_UNIQUE, NULL);
  }
  after_each() {
    rydb_close(db);
    rmdir_recursive(path);
  }
  it("deletes all files on rydb_delete") {
    assert_db_ok(db, rydb_open(db, path, "test"));
    assert(count_files(path) > 1);
    rydb_delete(db);
    asserteq(count_files(path), 1); //the directory itself, nothing else
  }
}

describe(concurrency) {
  static rydb_t *db;
  static char path[64];
  before_each() {
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
    config_testdb(db, 0);
  }
  after_each() {
    rmdir_recursive(path);
  }
  it("clears locks when database is closed") {
    assert_db_ok(db, rydb_open(db, path, "test"));
    rydb_close(db);
    db = rydb_new();
    assert_db_ok(db, rydb_open(db, path, "test"));
    rydb_close(db);
  }
  it("allows only one writer") {
    assert_db_ok(db, rydb_open(db, path, "test"));
    rydb_t *db2 = rydb_new();
    config_testdb(db2, 0);
    assert_db_fail(db2, rydb_open(db2, path, "test"), RYDB_ERROR_LOCK_FAILED);
    rydb_close(db);
    rydb_close(db2);
  }
  it("can be forced unlocked") {
    assert_db_ok(db, rydb_open(db, path, "test"));
    rydb_t *db2 = rydb_new();
    rydb_force_unlock(db);
    assert_db_ok(db2, rydb_open(db2, path, "test"));
    rydb_close(db);
    rydb_close(db2);
  }
}

describe(row_operations) {
  static rydb_t *db = NULL;
  static char path[64];
  
  static char *rowdata[] = {
    "1.hello this is not terribly long of a string",
    "2.and this is another one that exceeds the length",
    "3.this one's short",
    "4.tiny",
    "5.here's another one",
    "6.zzzzzzzzzzzzzz"
  };
  static int nrows = 6;
  
  before_each() {
    asserteq(db, NULL, "previous test not closed out correctly");
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
    config_testdb(db, 0);
    assert_db_ok(db, rydb_open(db, path, "test"));
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
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      assert_data_match(db, rowdata, nrows - 2);  
      
      //now insert the remainder
      assert_db_insert_rows(db, &rowdata[nrows-2], 2);
      
      assert_data_match(db, rowdata, nrows);
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
      assert_data_match(db, rowdata, nrows);
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
        asserteq(db->data_next_rownum, i);
        asserteq(db->cmd_next_rownum, i);
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
      //rydb_print_stored_data(db);
      for(int i=1; i<3; i++) {
        rydb_stored_row_t *row = rydb_rownum_to_row(db, nrows - i);
        row->type = RYDB_ROW_EMPTY;
      }
      rydb_stored_row_t *row = rydb_rownum_to_row(db, 1);
      row->type = RYDB_ROW_EMPTY;
      
      //now delete the last row, and see if data_next_row is updated correctly
      assert_db_ok(db, rydb_row_delete(db, nrows));
      asserteq(db->data_next_rownum, nrows-2);
      asserteq(db->cmd_next_rownum, nrows-2);
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
          asserteq(db->data_next_rownum, nrows+1);
          asserteq(db->cmd_next_rownum, nrows+1);
        }
        else {
          asserteq(db->data_next_rownum, 1);
          asserteq(db->cmd_next_rownum, 1);
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
      assert_data_match(db, rowdata, nrows);
      assert_data_rownum_type(db, nrows+1, RYDB_ROW_CMD_DELETE);
      assert_data_rownum_type(db, nrows+2, RYDB_ROW_CMD_DELETE);
      assert_data_rownum_type(db, nrows+3, RYDB_ROW_CMD_DELETE);
      assert_db_ok(db, rydb_transaction_finish(db));
      
      assert_data_match(db, rowdata, nrows - 1);
      
      //and it works fine afterwards
      assert_db_ok(db, rydb_row_insert_str(db, "after"));
      char *rowdata_results2[] = {
        rowdata[0], rowdata[1], rowdata[2], rowdata[3], rowdata[4], "after"
      };
      assert_data_match(db, rowdata_results2, nrows);
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
      char *rowdata_results[] = {
        rowdata[0], rowdata[3], rowdata[2], rowdata[1], rowdata[4], rowdata[5]
      };
      assert_data_match(db, rowdata_results, nrows);
    }
    
    it("swaps all the rows one by one") {
      assert_db_insert_rows(db, rowdata, nrows);
      //this should move the first row all the way down
      for(int i = 2; i <= nrows; i++) {
        assert_db_ok(db, rydb_row_swap(db, i, i-1));
      }
      //rydb_print_stored_data(db);
      char *rowdata_results[] = {
        rowdata[1], rowdata[2], rowdata[3], rowdata[4], rowdata[5], rowdata[0]
      };
      assert_data_match(db, rowdata_results, nrows);
    }
    
    it("swaps rows with an empty row") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_delete(db, 1));
      
      char *rowdata_results[] = {
        NULL, rowdata[1], rowdata[2], rowdata[3], rowdata[4], rowdata[5]
      };
      assert_data_match(db, rowdata_results, nrows);
      
      assert_db_ok(db, rydb_row_swap(db, 1, nrows-1));
      assert_db_ok(db, rydb_row_delete(db, nrows-2));
      assert_db_ok(db, rydb_row_swap(db, nrows, nrows-2));
      assert_db_ok(db, rydb_row_insert_str(db, "after"));
      
      char *rowdata_results2[] = {
        rowdata[4], rowdata[1], rowdata[2], rowdata[5], "after", NULL
      };
      assert_data_match(db, rowdata_results2, nrows);
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
      assert_db_fail(db, rydb_row_update(db, 1, "hey", ROW_LEN, 3), RYDB_ERROR_DATA_TOO_LARGE, "[Dd]ata length.* exceeds row length");
      assert_db_fail(db, rydb_row_update(db, 1, "zzzzzzzzzzzzzzzzzzzzzzzzzz", 0, ROW_LEN+1), RYDB_ERROR_DATA_TOO_LARGE);
    }
    
    it("updates a small part of a row") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_update(db, nrows, "hey", 3, 3));
      char *rowdata_results[] = {
        rowdata[0], rowdata[1], rowdata[2], rowdata[3], rowdata[4], "6.zheyzzzzzzzzzz"
      };
      assert_data_match(db, rowdata_results, nrows);
    }
    
    it("updates a large part of a row") {
      assert_db_insert_rows(db, rowdata, nrows);
      assert_db_ok(db, rydb_row_update(db, nrows, "heywhatis this even", 3, 17));
      assert_db_ok(db, rydb_row_update(db, nrows-1, "................................", 0, ROW_LEN));
      char *rowdata_results[] = {
        rowdata[0], rowdata[1], rowdata[2], rowdata[3], "....................", "6.zheywhatis this ev"
      };
      assert_data_match(db, rowdata_results, nrows);
    }
    
  }
}

describe(transactions) {
  static rydb_t *db = NULL;
  static char path[64];
  
  static char *rowdata[] = {
    "1.hello this is not terribly long of a string",
    "2.and this is another one that exceeds the length",
    "3.this one's short",
    "4.tiny",
    "5.here's another one",
    "6.zzzzzzzzzzzzzz"
  };
  static int nrows = sizeof(rowdata)/sizeof(char *);
  struct cmd_rownum_out_of_range_check_s rangecheck;
  
  before_each() {
    asserteq(db, NULL, "previous test not closed out correctly");
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
    config_testdb(db, 0);
    assert_db_ok(db, rydb_open(db, path, "test"));
    assert_db_insert_rows(db, rowdata, nrows);
  }
  after_each() {
    rydb_close(db);
    db = NULL;
    rmdir_recursive(path);
  }
  
  subdesc(commands) {
    subdesc(rownum) {
      it("fails SET with out-of-range rownum") {
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "SET",
          .rows = {{.type = RYDB_ROW_CMD_SET, .data="hows", .len = 4, .start = 0, .num = 1}},
          .n = 1, .n_check = 1
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      }
      it("fails UPDATE with out-of-range rownum") {
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "UPDATE",
          .rows = {{.type = RYDB_ROW_CMD_UPDATE, .data="hows", .len = 4, .start = 0, .num = 1}},
          .n = 1, .n_check = 1
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      }
      it("fails UPDATE2 with out-of-range rownum") {
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "UPDATE2", //2-part update
          .rows = {
            {.type = RYDB_ROW_CMD_UPDATE1, .data=NULL ,  .len = 4, .start = 0, .num = 1},
            {.type = RYDB_ROW_CMD_UPDATE2, .data="hows", .len = 4, .num = 1}
          },
          .n = 2, .n_check = 1
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      }
      it("fails DELETE with out-of-range rownum") {  
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "DELETE",
          .rows = {{.type = RYDB_ROW_CMD_DELETE, .data=NULL , .len = 4, .start = 0, .num = 1}},
          .n = 1, .n_check = 1
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      };
      it("fails SWAP1 + SWAP2 with out-of-range rownum") {
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "SWAP", //2-part update
          .rows = {
            {.type = RYDB_ROW_CMD_SWAP1, .data=NULL, .num = 4},
            {.type = RYDB_ROW_CMD_SWAP2, .data=NULL, .num = 2}
          },
          .n = 2, .n_check = 2
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      }
      it("fails SWAP1 + DELETE with out-of-range rownum") {
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "SWAP", //2-part update
          .rows = {
            {.type = RYDB_ROW_CMD_SWAP1, .data=NULL, .num = 4},
            {.type = RYDB_ROW_CMD_DELETE, .data=NULL, .num = 2}
          },
          .n = 2, .n_check = 2
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      }
      it("fails SWAP1 + SET with out-of-range rownum") {
        rangecheck = (struct cmd_rownum_out_of_range_check_s ){ .name = "SWAP", //2-part update
          .rows = {
            {.type = RYDB_ROW_CMD_SWAP1, .data=NULL, .num = 4},
            {.type = RYDB_ROW_CMD_SET, .data="wooooooooooooooooowwwww", .len=5, .start=0, .num = 2}
          },
          .n = 2, .n_check = 2
        };
        cmd_rownum_out_of_range_check(db, &rangecheck, nrows);
      }
    }
    
    
    subdesc(cmd_append) {
      it("fails if it can't grow the file") {
        rydb_row_t row = {.type = RYDB_ROW_CMD_DELETE, .num = 1};
        assert_db_ok(db, rydb_transaction_start(db));
        fclose(db->data.fp);
        db->data.fp = NULL;
        db->data.fd = -1;
        assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
        assert_db_fail(db, rydb_data_append_cmd_rows(db, &row, 1), RYDB_ERROR_FILE_SIZE, "[Ff]ailed to grow file");
      }
      it("fails if given a DATA row") {
        rydb_row_t row = {.type = RYDB_ROW_DATA};
        assert_db_fail(db, rydb_data_append_cmd_rows(db, &row, 1), RYDB_ERROR_TRANSACTION_FAILED, "append row .*DATA to transaction");
      }
      it("fails if given an EMPTY row") {
        rydb_row_t row = {.type = RYDB_ROW_EMPTY};
        assert_db_fail(db, rydb_data_append_cmd_rows(db, &row, 1), RYDB_ERROR_TRANSACTION_FAILED, "append row .*EMPTY to transaction");
      }
    }
    
    subdesc(SET) {
      it("assumes full row-length if .len==0") {
        assert_db_ok(db, rydb_transaction_start(db));
        rydb_row_t row = {.type = RYDB_ROW_CMD_SET, .data="abababababazazazaaaaazazaazaz", .len=0, .num=3};
        assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
      }
    }
    subdesc(UPDATE) {
      it("fails when UPDATE1 is the last command in the transaction") {
        assert_db_ok(db, rydb_transaction_start(db));
        rydb_row_t row = {.type = RYDB_ROW_CMD_UPDATE1, .data="abababa", .len=4, .num=3};
        assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
        assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "second .*command.* is missing"); 
      }
      it("fails when UPDATE1 is followed by anything but UPDATE2") {
        for(int i=0; i<255; i++) {
          if(i == RYDB_ROW_CMD_UPDATE2 || i == RYDB_ROW_DATA || i == RYDB_ROW_EMPTY) {
            continue;
          }
          assert_db_ok(db, rydb_transaction_start(db));
          rydb_row_t row = {.type = RYDB_ROW_CMD_UPDATE1, .data="abababa", .len=4, .num=3};
          assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
          row.type = i;
          assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
          assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "second .*wrong type.* is missing"); 
        }
      }
      
      it("fails when UPDATE2 is preceded by nothing") {
        rydb_close(db);
        db = rydb_new();
        config_testdb(db, 0);
        assert_db_ok(db, rydb_open(db, path, "UPDATE2_test"));
        rydb_row_t row = {.type = RYDB_ROW_CMD_UPDATE2, .data="abababa", .len=4, .num=3};
        assert_db_ok(db, rydb_transaction_start(db));
        assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
        assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "is missing"); 
      }
      
      it("fails when UPDATE2 is preceded by anything but UPDATE1") {
        for(int i=0; i<255; i++) {
          if(i == RYDB_ROW_CMD_UPDATE1  //has been tested
            || i == RYDB_ROW_CMD_UPDATE2  //same
            || i == RYDB_ROW_CMD_SWAP1 //will be tested
            || i == RYDB_ROW_CMD_SWAP2 //same
          ) {
            continue;
          }
          assert_db_ok(db, rydb_transaction_start(db));
          rydb_row_t row[] = {
            {.type = RYDB_ROW_CMD_UPDATE1, .data="abababazzzzzzzzzzzzzzzzzzzzzz", .len=4, .start=3, .num=3},
            {.type = RYDB_ROW_CMD_UPDATE2, .data="abababazzzzzzzzzzzzzzzzzzzzzz", .len=4, .start=3, .num=3}
          };
          assert_db_ok(db, rydb_data_append_cmd_rows(db, row, 2));
          rydb_stored_row_t *u1 = rydb_rownum_to_row(db, nrows + 1);
          u1->type = i;
          assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "wrong type"); 
        }
      }
    }
     subdesc(SWAP2) {
#ifdef RYDB_DEBUG
      it("fails when SWAP1 is the last command in the transaction") {
        rydb_debug_refuse_to_run_transaction_without_commit = 0;
        assert_db_ok(db, rydb_transaction_start(db));
        rydb_row_t row[] = {
          {.type = RYDB_ROW_CMD_SWAP1, .num=3}
        };
        assert_db_ok(db, rydb_data_append_cmd_rows(db, row, 1));
        assert_db_fail(db, rydb_transaction_run(db, NULL), RYDB_ERROR_TRANSACTION_FAILED, "SWAP.* missing"); 
        rydb_debug_refuse_to_run_transaction_without_commit = 1;
      }
#endif
      it("fails when SWAP1 is followed by anything but SWAP2") {
        for(int i=0; i<255; i++) {
          if(i == RYDB_ROW_CMD_SWAP2 || i == RYDB_ROW_DATA || i == RYDB_ROW_EMPTY
            || i == RYDB_ROW_CMD_SET || i == RYDB_ROW_CMD_DELETE) {
            continue;
          }
          assert_db_ok(db, rydb_transaction_start(db));
          rydb_row_t row = {.type = RYDB_ROW_CMD_SWAP1, .data="abababa", .len=4, .num=3};
          assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
          row.type = i;
          assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
          assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "second .*wrong type."); 
        }
      }
      
      it("fails when SWAP2 is preceded by nothing") {
        rydb_close(db);
        db = rydb_new();
        config_testdb(db, 0);
        assert_db_ok(db, rydb_open(db, path, "SWAP2_test"));
        rydb_row_t row = {.type = RYDB_ROW_CMD_SWAP2, .data="abababa", .len=4, .num=3};
        assert_db_ok(db, rydb_transaction_start(db));
        assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
        assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "SWAP1 is missing"); 
      }
      
      it("fails when SWAP2 is preceded by anything but SWAP1") {
        for(int i=0; i<255; i++) {
          if(i == RYDB_ROW_CMD_UPDATE1  //has been tested
            || i == RYDB_ROW_CMD_UPDATE2  //same
            || i == RYDB_ROW_CMD_SWAP1 //same
            || i == RYDB_ROW_CMD_SWAP2 //being tested now
          ) {
            continue;
          }
          assert_db_ok(db, rydb_transaction_start(db));
          rydb_row_t row[] = {
            {.type = RYDB_ROW_CMD_SWAP1, .data="abababazzzzzzzzzzzzzzzzzzzzzz", .len=4, .start=3, .num=3},
            {.type = RYDB_ROW_CMD_SWAP2, .data="abababazzzzzzzzzzzzzzzzzzzzzz", .len=4, .start=3, .num=3}
          };
          assert_db_ok(db, rydb_data_append_cmd_rows(db, row, 2));
          rydb_stored_row_t *u1 = rydb_rownum_to_row(db, nrows + 1);
          u1->type = i;
          //rydb_print_stored_data(db);
          assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "wrong type"); 
        }
      }
      
      it("fails to SWAP a non-empty non-data row") {
        assert_db_ok(db, rydb_transaction_start(db));
        rydb_row_t row[] = {
          {.type = RYDB_ROW_CMD_SWAP1, .len=4, .start=3, .num=3},
          {.type = RYDB_ROW_CMD_SWAP2, .len=4, .start=3, .num=1}
        };
        assert_db_ok(db, rydb_data_append_cmd_rows(db, row, 2));
        
        rydb_stored_row_t *s = rydb_rownum_to_row(db, 3);
        s->type = RYDB_ROW_CMD_UPDATE1;
        //rydb_print_stored_data(db);
        assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "wrong type"); 
        s->type = RYDB_ROW_DATA;
        assert_db_ok(db, rydb_data_append_cmd_rows(db, row, 2));
        
        assert_db_ok(db, rydb_transaction_start(db));
        s = rydb_rownum_to_row(db, 1);
        s->type = RYDB_ROW_CMD_UPDATE2;
        
        assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED, "wrong type");
        
        s->type = RYDB_ROW_DATA;
      }
      
      it("fails if SWAP1 fails") {
        assert_db_ok(db, rydb_transaction_start(db));
        assert_db_ok(db, rydb_row_swap(db, 1, 2));
        rydb_stored_row_t *s = rydb_rownum_to_row(db, nrows + 2);
        s->type = RYDB_ROW_EMPTY;
        assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_FAILED);
      }
    }
    
    subdesc(COMMIT) {
      it("refuses to run an uncommitted transaction") {
        assert_db_ok(db, rydb_transaction_start(db));
        assert_db_ok(db, rydb_row_swap(db, 1, 2));
        assert_db_ok(db, rydb_row_swap(db, 2, 3));
        assert_db_fail_match_errstr(db, rydb_transaction_run(db, NULL), RYDB_ERROR_TRANSACTION_INCOMPLETE, "doesn't end with a COMMIT");
      }
#ifdef RYDB_DEBUG
      it("notices if forced to run through an uncommitted transaction") {
        assert_db_ok(db, rydb_transaction_start(db));
        assert_db_ok(db, rydb_row_swap(db, 1, 2));
        assert_db_ok(db, rydb_row_swap(db, 2, 3));
        rydb_debug_refuse_to_run_transaction_without_commit = 0;
        assert_db_fail(db, rydb_transaction_run(db, NULL), RYDB_ERROR_TRANSACTION_FAILED, "committed without ending on a COMMIT");
        rydb_debug_refuse_to_run_transaction_without_commit = 1;
      }
#endif
    }
    
    it("refuses to finish a transaction that wasn't started") {
      assert_db_fail(db, rydb_transaction_finish(db), RYDB_ERROR_TRANSACTION_INACTIVE);
    }
    it("refuses to start a transaction twice") {
      assert_db_ok(db, rydb_transaction_start(db));
      assert_db_fail(db, rydb_transaction_start(db), RYDB_ERROR_TRANSACTION_ACTIVE, "already active");
    }
    
    it("refuses to cancel an inactive transaction") {
      assert_db_fail(db, rydb_transaction_cancel(db), RYDB_ERROR_TRANSACTION_INACTIVE, "[Nn]o active transaction");
    }
    it("cancels an ongoing transaction") {
      assert_db_ok(db, rydb_transaction_start(db));
      assert_db_ok(db, rydb_row_delete(db, 1));
      assert_db_ok(db, rydb_row_delete(db, 2));
      assert_db_ok(db, rydb_transaction_cancel(db));
      
      assert_data_match(db, rowdata, nrows);
    }
  }
  
#define RYDB_ROW_TXTEST_COMMANDS(db) \
  assert_db_ok(db, rydb_row_swap(db, 1, 2)); \
  assert_db_ok(db, rydb_row_delete(db, 2)); \
  assert_db_ok(db, rydb_row_delete(db, 6)); \
  assert_db_ok(db, rydb_row_insert_str(db, "7.an insertion")); \
  assert_db_ok(db, rydb_row_swap(db, 5, 6))
  
  subdesc(restore_log) {
    static char *result_rowdata[] = {
      "2.and this is another one that exceeds the length",
      NULL,
      "3.this one's short",
      "4.tiny",
      NULL,
      "5.here's another one",
      "7.an insertion",
    };
    static int result_nrows = 7;
    
    it("runs committed commands") {
      assert_db_ok(db, rydb_transaction_start(db));
      RYDB_ROW_TXTEST_COMMANDS(db);
      rydb_row_t row = {.type = RYDB_ROW_CMD_COMMIT};
      assert_db_ok(db, rydb_data_append_cmd_rows(db, &row, 1));
      assert_data_match(db, rowdata, nrows);
      //rydb_print_stored_data(db);
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      //rydb_print_stored_data(db);
      
      assert_data_match(db, result_rowdata, result_nrows);
    }
    it("discards uncommitted commands") {
      assert_db_ok(db, rydb_transaction_start(db));
      RYDB_ROW_TXTEST_COMMANDS(db);
      assert_data_match(db, rowdata, nrows);
      //rydb_print_stored_data(db);
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      //rydb_print_stored_data(db);
      
      assert_data_match(db, rowdata, nrows);
    }
    
    it("runs commands up to the last COMMIT but no further") {
      rydb_row_t commit = {.type = RYDB_ROW_CMD_COMMIT};
      assert_db_ok(db, rydb_data_append_cmd_rows(db, &commit, 1));
      assert_db_ok(db, rydb_transaction_start(db));
      RYDB_ROW_TXTEST_COMMANDS(db);
      assert_data_match(db, rowdata, nrows);
      assert_db_ok(db, rydb_data_append_cmd_rows(db, &commit, 1));
      assert_db_ok(db, rydb_row_insert_str(db, "yeaps"));
      assert_db_ok(db, rydb_data_append_cmd_rows(db, &commit, 1));
      assert_db_ok(db, rydb_row_delete(db, 1)); //this should be discarded
      assert_db_ok(db, rydb_row_swap(db, 5, 6)); //and this
      // (because a commit does not follow
      //don't finish the transaction
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      char *results[] = {
        rowdata[1], NULL, rowdata[2], rowdata[3], NULL, rowdata[4], "7.an insertion", "yeaps"
      };
      assert_data_match(db, results, 8);
    }
    
    it("truncates the filesize to the end of the data rows") {
      off_t old_sz = filesize(db->data.path);
      assert_db_ok(db, rydb_transaction_start(db));
      assert_db_ok(db, rydb_row_swap(db, 1, 2));
      assert_db_ok(db, rydb_row_delete(db, 2));
      assert_db_ok(db, rydb_row_delete(db, 6));
      assert_db_ok(db, rydb_row_swap(db, 5, 6));
      //rydb_print_stored_data(db);
      //don't finish the transaction
      rydb_close(db);
      
      db = rydb_new();
      config_testdb(db, 0);
      assert_db_ok(db, rydb_open(db, path, "test"));
      //rydb_print_stored_data(db);
      assert_data_match(db, rowdata, nrows);
      int n=0;
      RYDB_EACH_ROW(db, cur) {
        n++;
      }
      off_t new_sz = filesize(db->data.path);
      assert(new_sz <= old_sz);
      asserteq(n, nrows, "no rows should follow data after a reload");
    }
  }
  
  subdesc(uniqueness_constraints) {
    static const char *fmt = "%i.........yeah?................";
    static char buf[64];
    
    it("adds uniqueness constraints on insert") {
      int max = 1000 * repeat_multiplier;
      assert_db_ok(db, rydb_transaction_start(db));
      for(int i=0; i<max; i++) {
        sprintf(buf, fmt, i);
        assert_db_ok(db, rydb_row_insert_str(db, buf));
      }
      for(int i=0; i<max; i++) {
        sprintf(buf, fmt, i);
        assert_db_fail(db, rydb_row_insert_str(db, buf), RYDB_ERROR_NOT_UNIQUE, "primary must be unique");
      }
      assert_db_ok(db, rydb_transaction_cancel(db));
      for(int i=0; i<max; i++) {
        sprintf(buf, fmt, i);
        assert_db_ok(db, rydb_row_insert_str(db, buf));
      }
    }
    
    it("removes constraints on delete") {
      assert_db_ok(db, rydb_transaction_start(db));
      for(int i=0; i<nrows; i++) {
        assert_db_ok(db, rydb_row_delete(db, i+1));
      }
      for(int i=0; i<nrows; i++) {
        assert_db_ok(db, rydb_row_insert_str(db, rowdata[i]));
        assert_db_fail(db, rydb_row_insert_str(db, rowdata[i]), RYDB_ERROR_NOT_UNIQUE, "must be unique");
      }
    }
    
    it("modifies constraints on update") {
      assert_db_ok(db, rydb_transaction_start(db));
      for(int i=0; i<nrows; i++) {
        sprintf(buf, fmt, i);
        assert_db_ok(db, rydb_row_update(db, i+1, buf, 0, 10));
      }
      for(int i=0; i<nrows; i++) {
        sprintf(buf, fmt, i);
        assert_db_fail(db, rydb_row_insert_str(db, buf), RYDB_ERROR_NOT_UNIQUE, "must be unique");
        assert_db_ok(db, rydb_row_insert_str(db, rowdata[i]));
        assert_db_fail(db, rydb_row_insert_str(db, rowdata[i]), RYDB_ERROR_NOT_UNIQUE, "must be unique");
      }
    }
  }
}

describe(hashtable) {
  static rydb_t *db;
  static char path[256];
  
  before_each() {
    db = rydb_new();
    assert_db_ok(db, rydb_config_row(db, ROW_LEN, 5));
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
  }

  after_each() {
    rydb_close(db);
    db = NULL;
    rmdir_recursive(path);
#ifdef RYDB_DEBUG
    rydb_debug_hash_key = NULL;
#endif
  }
  
  test("adding rows to hashtable") {
    assert_db_ok(db, rydb_open(db, path, "test"));
    char str[128];
    for(int i=1; i<200; i++) {
      sprintf(str, "%izzz", i);
      assert_db_ok(db, rydb_row_insert_str(db, str));
      //rydb_hashtable_print(db, &db->index[0]);
    }
  }
  

  static char testname[128];
  static int hashfunction[] = {
    RYDB_HASH_SIPHASH, RYDB_HASH_CRC32, RYDB_HASH_NOHASH
  };
  static int t, start, rh;
  static uint8_t rehash[] = {RYDB_REHASH_ALL_AT_ONCE, RYDB_REHASH_MANUAL, RYDB_REHASH_INCREMENTAL};
  static char   *rehash_name[] = {"all-at-once", "manual", "incremental"};
  for(rh=0; rh<3; rh++) {
    for(start=0; start <=9; start+=9) {
      for(t=0; t<3; t++) {
        sprintf(testname, "finding rows (index start at %i) in %s %s-rehash hashtable", start, rydb_hashfunction_to_str(hashfunction[t]), rehash_name[rh]);
        test(testname) {
          rydb_config_index_hashtable_t cf = {
            .rehash = rehash[rh],
            .hash_function = hashfunction[t],
            .store_value = 0,
            .store_hash = 1,
            .collision_resolution = RYDB_OPEN_ADDRESSING
          };
          rydb_config_index_hashtable_t cf2 = cf;
          cf2.store_value = 1;
          assert_db_ok(db, rydb_config_add_index_hashtable(db, "primary", start, 5, RYDB_INDEX_UNIQUE, &cf));
          assert_db_ok(db, rydb_config_add_index_hashtable(db, "secondary", start, 5, RYDB_INDEX_DEFAULT, &cf2));
          assert_db_ok(db, rydb_open(db, path, "test"));
          char str[128], searchstr[128];
          const char *fmt = "%i,%i!%i|%i&%i*%i~%i@%i$%i*%i!";
          asserteq(rydb_find_row_str(db, "nil", NULL), 0);
          
          int maxrows = 1000 * repeat_multiplier;
          for(int i=0; i<maxrows; i++) {
            //printf("i: %i\n", i);
            sprintf(str, fmt, i, i, i, i, i, i, i, i, i, i);
            memset(&str[ROW_LEN], '\00', 128 - ROW_LEN);
            assert_db_ok(db, rydb_row_insert_str(db, str));
            //rydb_hashtable_print(db, &db->index[0]);
            
            if(rehash[rh] == RYDB_REHASH_MANUAL && i%(maxrows/10) == 0) {
              assert_db_ok(db, rydb_index_rehash(db, "primary"));
              assert_db_ok(db, rydb_index_rehash(db, "secondary"));
            }
            for(int j=0; j<=i; j++) {
              sprintf(searchstr, fmt, j, j, j, j, j, j, j, j, j, j);
              memset(&searchstr[ROW_LEN], '\00', 128 - ROW_LEN);
              rydb_row_t found_row, found_row2;
              //raise(SIGSTOP);
              //printf("i: %i, j: %i, finding \"%s\"\n", i, j, &searchstr[start]);
              int found = rydb_find_row_str(db, &searchstr[start], &found_row);
              int found_secondary = rydb_index_find_row_str(db, "secondary", &searchstr[start], &found_row2);
              if (j <= i) {
                asserteq(found, 1);
                asserteq(found_secondary, 1);
                asserteq(strcmp(searchstr, found_row.data), 0);
                asserteq(strcmp(searchstr, found_row2.data), 0);
                asserteq(found_row.num, j+1);
                asserteq(found_row2.num, j+1);
              }
              else {
                asserteq(found, 0);
                asserteq(found_secondary, 0);
              }
            }
          }
        }
      }
    }
  }
  for(t=0; t<3; t++) {
    sprintf(testname, "delete rows in hashtable %s", rydb_hashfunction_to_str(hashfunction[t]));
    test(testname) {
      rydb_config_index_hashtable_t cf = {
        .hash_function = hashfunction[t],
        .store_value = 0,
        .store_hash = 1,
        .collision_resolution = RYDB_OPEN_ADDRESSING
      };
      rydb_config_index_hashtable_t cf2 = cf;
      cf2.store_value = 1;
      rydb_config_add_index_hashtable(db, "primary", 0, 5, RYDB_INDEX_UNIQUE, &cf);
      rydb_config_add_index_hashtable(db, "secondary", 0, 5, RYDB_INDEX_DEFAULT, &cf2);
      assert_db_ok(db, rydb_open(db, path, "test"));
      char str[128];
      char *fmt = "%izzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
      int numrows = 1000 * repeat_multiplier;
      for(int i=1; i<=numrows; i++) {
        sprintf(str, fmt, i);
        assert_db_ok(db, rydb_row_insert_str(db, str));
      }
      hashtable_header_count_check(db, &db->index[0], numrows);
      
      for(int i=1; i<=numrows; i++) {
        sprintf(str, fmt, i);
        assert_db_ok(db, rydb_row_delete(db, i));
        hashtable_header_count_check(db, &db->index[0], numrows - i);
        //rydb_hashtable_print(db, &db->index[0]);
        for(int j=1; j<=numrows; j++) {
          sprintf(str, fmt, j);
          rydb_row_t found_row, found_row2;
          int        rc, rc2;
          //printf("find %s\n", str);
          rc = rydb_find_row_str(db, str, &found_row);
          rc2 = rydb_index_find_row_str(db, "secondary", str, &found_row2);
          assert_db(db);
          if(j <= i) {
            asserteq(rc, 0);
            asserteq(rc2, 0);
          }
          else {
            assert(rc);
            assert(rc2);
            asserteq(found_row.num, j);
            asserteq(found_row2.num, j);
          }
        }
      }
    }
  }
  
  it("obeys uniqueness criteria") {
    //char str[128];
    assert_db_ok(db, rydb_open(db, path, "test"));
    assert_db_ok(db, rydb_row_insert_str(db, "hello this is a string"));
    assert_db_ok(db, rydb_row_insert_str(db, "oh this is a different string"));
    assert_db_ok(db, rydb_row_insert_str(db, "samestring"));
    assert_db_fail(db, rydb_row_insert_str(db, "samestring"), RYDB_ERROR_NOT_UNIQUE, "primary must be unique");
  }
}

describe(storage) {
  static rydb_t *db;
  static char path[64];
  static char *data;
  static int  maxlen = 12000;
  before_each() {
    db = rydb_new();
    strcpy(path, "test.db.XXXXXX");
    mkdtemp(path);
    data = malloc(maxlen);
  }
  after_each() {
    rydb_close(db);
    rmdir_recursive(path);
    free(data);
  }
  
  static char testname[128];
  static int rowlen;

  static int max_rownum = 500;
  for(rowlen = 1; rowlen < maxlen; rowlen+=rowlen< 15 ? 1 : (rowlen<50 ? 9 : 613)) {
    sprintf(testname, "storage with row length %i", rowlen);
    test(testname) {
      config_testdb(db, rowlen);
      assert_db_ok(db, rydb_open(db, path, "test"));
      int rownum = max_rownum;
      if(rowlen == 1) rownum = MIN(9, rownum);
      else if(rowlen == 2) rownum = MIN(99, rownum);
      else if(rowlen == 3) rownum = MIN(999, rownum);
      else if(rowlen == 4) rownum = MIN(9999, rownum);
      
      for(int i=1; i<=rownum; i++) {
        data_fill(data, rowlen, i);
        assert_db_ok(db, rydb_row_insert_str(db, data));
      }
      for(int i=1; i<=rownum + 4; i++) {
        rydb_row_t row;
        int rc = rydb_find_row_at(db, i, &row);
        //printf("%i\n", i);
        if(i<=rownum) {
          data_fill(data, rowlen, i);
          asserteq(rc, 1);
          
          asserteq(memcmp(data, row.data, rowlen), 0);
        }
        else if(i == rownum + 1) {
          asserteq(rc, 1);
          asserteq(row.type, RYDB_ROW_EMPTY);
        }
        else {
          asserteq(rc, 0);
        }
      }
    }
  }
}
int set_test_options(int *argc, char **argv) {
  int i = 1;
  while(i < *argc) {
    char *arg = argv[i];
    if(strcmp(arg, "--multiplier") == 0 && *argc >= i+1) {
      char *val = argv_extract2(argc, argv, i);
      if((repeat_multiplier = atof(val)) == 0.0) {
        printf("invalid --multiplier value %s\n", val);
        return 0;
      }
    }
    else {
      i++;
    }
  }
  return 1;
}

snow_main_decls;
int main(int argc, char **argv) {
  if(!set_test_options(&argc, argv)) {
    return 1;
  }
  return snow_main_function(argc, argv);
}
