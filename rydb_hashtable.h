#ifndef _RYDB_HASHTABLE_H
#define _RYDB_HASHTABLE_H
#include "rydb.h"

int rydb_index_hashtable_open(rydb_t *db, rydb_index_t *idx);

int rydb_meta_load_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp);
int rydb_meta_save_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp);

int rydb_config_index_hashtable_set_config(rydb_t *db, rydb_config_index_t *idx_cf, rydb_config_index_hashtable_t *advanced_config);


int rydb_index_hashtable_add_row(rydb_t *db, rydb_index_t *idx, rydb_stored_row_t *row);
int rydb_index_hashtable_remove_row(rydb_t *db, rydb_index_t *idx, rydb_stored_row_t *row);
int rydb_index_hashtable_update_add_row(rydb_t *db,  rydb_index_t *idx, rydb_stored_row_t *row, off_t start, off_t end);
int rydb_index_hashtable_update_remove_row(rydb_t *db,  rydb_index_t *idx, rydb_stored_row_t *row, off_t start, off_t end);


typedef struct {
  struct {
      size_t        sz;
      struct {
        size_t        total_count;
        size_t        used_count;
      }             count;
  }               bucket;
  uint16_t        incomplete_migration_count;
}
rydb_hashtable_header_t;
#endif //_RYDB_HASHTABLE_H
