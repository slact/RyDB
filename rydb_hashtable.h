#ifndef _RYDB_HASHTABLE_H
#define _RYDB_HASHTABLE_H
#include "rydb.h"

int rydb_meta_load_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp);
int rydb_meta_save_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp);

int rydb_config_index_hashtable_set_config(rydb_t *db, rydb_config_index_t *idx_cf, rydb_config_index_hashtable_t *advanced_config);
#endif //_RYDB_HASHTABLE_H
