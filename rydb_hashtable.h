#ifndef _RYDB_HASHTABLE_H
#define _RYDB_HASHTABLE_H
#include "rydb.h"

#define RYDB_HASHTABLE_DEFAULT_MAX_LOAD_FACTOR 0.60
#define RYDB_HASHTABLE_DEFAULT_REHASH_FLAGS RYDB_REHASH_INCREMENTAL
bool rydb_index_hashtable_open(rydb_t *db, rydb_index_t *idx);

bool rydb_meta_load_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp);
bool rydb_meta_save_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp);

bool rydb_config_index_hashtable_set_config(rydb_t *db, rydb_config_index_t *idx_cf, rydb_config_index_hashtable_t *advanced_config);


bool rydb_index_hashtable_add_row(rydb_t *db, rydb_index_t *idx, rydb_stored_row_t *row);
bool rydb_index_hashtable_remove_row(rydb_t *db, rydb_index_t *idx, rydb_stored_row_t *row);

void rydb_hashtable_reserve(const rydb_index_t *idx);
void rydb_hashtable_release(const rydb_index_t *idx);

bool rydb_index_hashtable_contains(const rydb_t *db, const rydb_index_t *idx, const char *val);

bool rydb_index_hashtable_find_row(rydb_t *db, rydb_index_t *idx, const char *val, rydb_row_t *row);
bool rydb_index_hashtable_rehash(rydb_t *db, rydb_index_t *idx, off_t last_possible_bucket, uint_fast8_t current_hashbits, int reserve);

char *rydb_hashfunction_to_str(rydb_hash_function_t hashfn);
typedef struct {
  rydb_rownum_t count;
  uint8_t       bits;
  /* whoa that's a lot of padding at the end there here...*/
}rydb_hashtable_bitlevel_count_t;

#define RYDB_HASHTABLE_BUCKET_MAX_BITLEVELS (8*sizeof(rydb_rownum_t) + 1)

typedef struct {
  int8_t          reserved; //reserved for writing
  uint8_t         active;
  struct {
    struct {
      rydb_rownum_t   total;
      rydb_rownum_t   used;
      rydb_rownum_t   load_factor_max;
      uint8_t         bitlevels;
    }               count;
    rydb_hashtable_bitlevel_count_t bitlevel[RYDB_HASHTABLE_BUCKET_MAX_BITLEVELS];
  }               bucket;
} rydb_hashtable_header_t;

typedef char rydb_hashbucket_t;
#define BUCKET_STORED_ROWNUM(bucket) *(rydb_rownum_t *)bucket
#define BUCKET_STORED_VALUE(bucket, cf) (char *)&bucket[sizeof(rydb_rownum_t) + (cf->type_config.hashtable.store_hash ? sizeof(uint64_t) : 0)]
#define BUCKET_NUMBER(bucket, idx) ((bucket - hashtable_bucket(idx, 0)) / bucket_size(idx->config))

void rydb_hashtable_print(const rydb_t *db, const rydb_index_t *idx);
void rydb_bucket_print(const rydb_index_t *idx, const rydb_hashbucket_t *bucket);

#define RYDB_INDEX_HASHTABLE_START_OFFSET ry_align(sizeof(rydb_hashtable_header_t), 8)

#endif //_RYDB_HASHTABLE_H
