#include <rydb_internal.h>
#include <rydb_hashtable.h>
#include <criterion/criterion.h>
#include "test_util.h"

Test(db_init, new) {
  fail_malloc_after(0);
  cr_assert_eq(rydb_new(), NULL);
  reset_malloc();
}

Test(db_config, row) {
  rydb_t *db = rydb_new();
  cr_assert_db(db);
  cr_assert_db_fail(db, rydb_config_row(db, 10, 20), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_row(db, 0, 0), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_row(db, RYDB_ROW_LEN_MAX+1, 0), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_ok(db,   rydb_config_row(db, 10, 5));
  cr_assert_eq(db->config.row_len, 10);
  cr_assert_eq(db->config.id_len, 5);
  rydb_close(db);
}

Test(db_config, row_link) {
  rydb_t *db = rydb_new();
  cr_assert_db(db);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "", "meh"), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "meh", ""), RYDB_ERROR_BAD_CONFIG);
  
  fail_malloc_after(0);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
  cr_assert_eq(db->config.link_pair_count, 0);
  fail_malloc_after(1);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
  cr_assert_eq(db->config.link_pair_count, 0);
  fail_malloc_after(2);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "foo", "bar"), RYDB_ERROR_NOMEMORY);
  cr_assert_eq(db->config.link_pair_count, 0);
  reset_malloc();
  
  fail_malloc_after(6);
  cr_assert_db_ok(db, rydb_config_add_row_link(db, "foo", "bar"));
  cr_assert_eq(db->config.link_pair_count, 1);
  cr_assert_db_ok(db, rydb_config_add_row_link(db, "foo2", "bar2"));
  cr_assert_eq(db->config.link_pair_count, 2);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "foo3", "bar3"), RYDB_ERROR_NOMEMORY);
  cr_assert_eq(db->config.link_pair_count, 2);
  cr_assert_neq(db->config.link, NULL);
  reset_malloc();
  
  char bigname[RYDB_NAME_MAX_LEN+10];
  memset(bigname, 'z', sizeof(bigname));
  bigname[sizeof(bigname)-1]='\00';
  cr_assert_db_fail(db, rydb_config_add_row_link(db, bigname, "meh"), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "meh", bigname), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "non-alphanum!", "meh"), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "meh", "non-alphanum!"), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "same", "same"), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_db_ok(db, rydb_config_add_row_link(db, "next", "prev"));
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "next", "meh"), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "meh", "next"), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "prev", "meh"), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "meh", "prev"), RYDB_ERROR_BAD_CONFIG);
  rydb_close(db);
  
  db = rydb_new();
  cr_assert_db(db);
  for(int i=0; i<RYDB_ROW_LINK_PAIRS_MAX; i++) {
    char prevname[32], nextname[32];
    sprintf(prevname, "prev%i", i);
    sprintf(nextname, "next%i", i);
    cr_assert_db_ok(db, rydb_config_add_row_link(db, nextname, prevname));
  }
  
  //too many links
  cr_assert_db_fail(db, rydb_config_add_row_link(db, "next1000", "prev1000"), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_eq(db->config.link_pair_count, RYDB_ROW_LINK_PAIRS_MAX);
  
  const char *cur = NULL, *prev="\00";
  char cpy[32];
  rydb_config_row_link_t *links = db->config.link;
  for(int i=0; i<RYDB_ROW_LINK_PAIRS_MAX * 2; i++) {
    cur = links[i].next;
    strcpy(cpy, cur);

    memcpy(cpy, cur[0]=='n' ? "prev" : "next", 4);
    cr_expect_gt(strcmp(cur, prev), 0, "row links are supposed to be sorted");
    cr_expect_eq(strcmp(links[i].prev, cpy), 0);
    cr_expect_eq(links[i].inverse, cur[0]=='n' ? 0 : 1);
    
    prev = cur;
  }
  
  rydb_close(db);
}

void test_errhandler(rydb_t *db, rydb_error_t *err, void *privdata) {
  cr_expect_eq(db, privdata);
  cr_expect_eq(err->code, RYDB_ERROR_BAD_CONFIG);
}

Test(db_config, error_handler) {
  rydb_t *db = rydb_new();
  rydb_set_error_handler(db, test_errhandler, db);
  cr_assert_db_fail(db, rydb_config_row(db, 0, 0), RYDB_ERROR_BAD_CONFIG);
  rydb_close(db);
}

Test(db_config, hashtable_index) {
  rydb_t *db = rydb_new();
  rydb_config_row(db, 20, 5);
  
  //bad flags
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, 0xFF, NULL), RYDB_ERROR_BAD_CONFIG);
  
  rydb_config_index_hashtable_t cf = {
    .hash_function = -1,
    .store_value = 1,
    .direct_mapping = 1
  };
  //bad hashtable config
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, &cf), RYDB_ERROR_BAD_CONFIG);
  
  char bigname[RYDB_NAME_MAX_LEN+10];
  memset(bigname, 'z', sizeof(bigname)-1);
  bigname[sizeof(bigname)-1]='\00';
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, bigname, 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "shouldn't have spaces", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 0, 30, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 30, 1, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  
  cr_assert_db_ok(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL));
  //can't add duplicate index
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  
  //primary index must be unique
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  
  rydb_close(db);
  
  db = rydb_new();
  rydb_config_row(db, 20, 5);
  //add all the possible indices
  for(int i=0; i < RYDB_INDICES_MAX-1; i++) {
    char indexname[32];
    sprintf(indexname, "index%i", i);
    cr_assert_db_ok(db, rydb_config_add_index_hashtable(db, indexname, 5, 5, RYDB_INDEX_DEFAULT, NULL));
  }
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  cr_assert_db_ok(db, rydb_config_add_index_hashtable(db, "primary", 5, 5, RYDB_INDEX_UNIQUE, NULL));
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar2", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_BAD_CONFIG);
  //check out the indices
  cr_assert_eq(db->config.index_count, RYDB_INDICES_MAX);
  rydb_config_index_t *cur, *prev = NULL;
  for(int i=0; i<RYDB_INDICES_MAX; i++) {
    cur = &db->config.index[i];
    if(prev) {
      cr_expect_gt(strcmp(cur->name, prev->name), 0, "indices are supposed to be sorted");
    }
    prev = cur;
  }
  rydb_close(db);
  
  //memory allocation tests
  db = rydb_new();
  rydb_config_row(db, 20, 5);
  fail_malloc_after(0);
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_NOMEMORY);
  fail_malloc_after(1);
  cr_assert_db_fail(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL), RYDB_ERROR_NOMEMORY);
  fail_malloc_after(2);
  cr_assert_db_ok(db, rydb_config_add_index_hashtable(db, "foobar", 5, 5, RYDB_INDEX_DEFAULT, NULL));
  reset_malloc();
}

Test(db_config, db_revision) {
  rydb_t *db = rydb_new();
  cr_assert_db_fail(db, rydb_config_revision(db, RYDB_REVISION_MAX + 1), RYDB_ERROR_BAD_CONFIG);
  cr_assert_eq(db->config.revision, 0);
  cr_assert_db_ok(db, rydb_config_revision(db, 15));
  cr_assert_eq(db->config.revision, 15);
  rydb_close(db);
}


Test(db_init, open) {
  rydb_t *db = rydb_new();
  rydb_config_row(db, 20, 5);
  
  cr_assert_db_fail(db, rydb_config_revision(db, RYDB_REVISION_MAX + 1), RYDB_ERROR_BAD_CONFIG);
  cr_assert_eq(db->config.revision, 0);
  cr_assert_db_ok(db, rydb_config_revision(db, 15));
  cr_assert_eq(db->config.revision, 15);
  rydb_close(db);
}

Test(hash, siphash_2_4) {
  uint8_t in[64], k[16], out[8];

  //initialize key
  for (int i = 0; i < 16; ++i) k[i] = i;
  
  for (int i = 0; i < 64; ++i) {
    in[i] = i;
    *(uint64_t *)out = siphash(in, i, k);
    cr_assert_eq(memcmp(out, vectors_siphash_2_4_64[i], 8), 0, "mismatch at test vector %i", i);
  }
}
