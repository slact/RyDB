#ifndef _RYDB_H
#define _RYDB_H
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>

#define RYDB_FORMAT_VERSION 1

typedef uint32_t rydb_rownum_t;
#define RYDB_ROWNUM_MAX  ((rydb_rownum_t ) -1)
#define RYDB_ROWNUM_NULL ((rydb_rownum_t ) 0)

#define RYDB_ROW_LINK_PAIRS_MAX 16

#define RYDB_NAME_MAX_LEN 64
#define RYDB_INDICES_MAX 32
#define RYDB_REVISION_MAX UINT32_MAX
#define RYDB_ROW_LEN_MAX UINT16_MAX

#define RYDB_INDEX_DEFAULT  0x0
#define RYDB_INDEX_UNIQUE   0x1

typedef struct {
  char           *str;
  uint16_t        len;
} rydb_str_t;

typedef struct {
  uint8_t     type;
  uint8_t     reserved;
  char        data[]; //cool c99 bro
} rydb_row_t;

typedef struct {
  char *start;
  char *end;
} rydb_char_range_t;

typedef struct {
  int               fd;
  FILE             *fp;
  rydb_char_range_t mmap;
  rydb_char_range_t file;
  rydb_char_range_t data;
  const char       *path;
} rydb_file_t;

typedef enum {
  RYDB_INDEX_INVALID = 0,
  RYDB_INDEX_HASHTABLE = 1,
  RYDB_INDEX_BTREE = 2
} rydb_index_type_t;

typedef struct {
  enum {
    RYDB_HASH_CRC32 =     0,
    RYDB_HASH_NOHASH =    1, //treat the value as if it's already a good hash
    RYDB_HASH_SIPHASH =   2
  }            hash_function;
  
  //storing the value in the hashtable prevents extra datafile reads at the cost of possibly much larger hashtable entries
  unsigned     store_value: 1;
  
  //direct mapping uses open-address linear probing, ideal for a 1-to-1 unique primary index. <2 reads avg.
  unsigned     direct_mapping: 1;
} rydb_config_index_hashtable_t;


typedef union {
  rydb_config_index_hashtable_t hashtable;
} rydb_config_index_type_t;


typedef struct {
  struct {
    rydb_rownum_t numrows;
    uint8_t       bucket_bits;
    uint8_t       bucket_lazy_bits;
  }             base;
  struct {
    rydb_rownum_t buckets;
    uint8_t       load_factor;
    size_t        datasize;
  }             derived;
  uint64_t    (*hash_function)(const char *data, size_t data_len);
} rydb_index_state_hashtable_t;

typedef union {
  rydb_index_state_hashtable_t hashtable;
} rydb_index_state_t;

typedef struct {
  const char        *name;
  rydb_index_type_t  type;
  uint16_t           start; // start of indexable value in row
  uint16_t           len; //length of indexable data
  rydb_config_index_type_t type_config;
  uint8_t            flags;
} rydb_config_index_t;

typedef struct {
  rydb_file_t          index;
  rydb_file_t          data;
  rydb_index_state_t   state;
} rydb_index_t;

typedef struct {
  uint16_t next;
  uint16_t prev;
} rydb_row_link_t;

typedef struct {
  const char *next;
  const char *prev;
  unsigned    inverse: 1;
} rydb_config_row_link_t;

#define RYDB_ERROR_MAX_LEN 1024
typedef enum {
  RYDB_NO_ERROR                   = 0,
  RYDB_ERROR_UNSPECIFIED          = 1,
  RYDB_ERROR_NOMEMORY             = 2,
  RYDB_ERROR_FILE_NOT_FOUND       = 3,
  RYDB_ERROR_FILE_EXISTS          = 4,
  RYDB_ERROR_LOCK_FAILED          = 5,
  RYDB_ERROR_FILE_ACCESS          = 6,
  RYDB_ERROR_FILE_INVALID         = 7,
  RYDB_ERROR_FILE_SIZE            = 8,
  RYDB_ERROR_CONFIG_MISMATCH      = 9,
  RYDB_ERROR_VERSION_MISMATCH     = 10,
  RYDB_ERROR_REVISION_MISMATCH    = 11,
  RYDB_ERROR_BAD_CONFIG           = 12,
  RYDB_ERROR_WRONG_ENDIANNESS     = 13
} rydb_error_code_t;

typedef struct {
  rydb_error_code_t    code;
  int                  errno_val;
  char                 str[RYDB_ERROR_MAX_LEN];
} rydb_error_t;

typedef struct {
  uint32_t revision;
  uint16_t row_len;
  uint16_t id_len; //id is part of the row, starting at row[0]
  uint16_t link_pair_count;
  rydb_config_row_link_t *link;
  uint16_t index_count;
  rydb_config_index_t *index;
} rydb_config_t;

typedef struct rydb_s rydb_t;
struct rydb_s {
  const char     *path;
  const char     *name;
  rydb_file_t     data;
  rydb_file_t     meta;
  rydb_config_t   config;
  rydb_index_t   *index;
  void           *transaction; //TODO
  unsigned        lock_acquired: 1;
  struct {
    void        (*function)(rydb_t *db, rydb_error_t *err, void *pd);
    void         *privdata;
  }               error_handler;
  rydb_error_t    error;
  
};// rydb_t

rydb_t *rydb_new(void);

int rydb_config_row(rydb_t *db, unsigned row_len, unsigned id_len);
int rydb_config_revision(rydb_t *db, unsigned revision);
int rydb_config_add_row_link(rydb_t *db, const char *link_name, const char *reverse_link_name);
int rydb_config_add_index_hashtable(rydb_t *db, const char *name, unsigned start, unsigned len, uint8_t flags, rydb_config_index_hashtable_t *advanced_config);

int rydb_set_error_handler(rydb_t *db, void (*fn)(rydb_t *, rydb_error_t *, void *), void *pd);

int rydb_open(rydb_t *db, const char *path, const char *name);


rydb_row_t *rydb_row_find(rydb_t *db, const char *id); //return 1 if found, 0 if not found

int rydb_row_safe_to_write_directly_check(rydb_t *db, rydb_row_t *row, off_t start, size_t len);
int rydb_row_insert(rydb_t *db, rydb_row_t *row);
int rydb_row_delete(rydb_t *db, rydb_row_t *row);
int rydb_row_update(rydb_t *db, rydb_row_t *row, off_t start, size_t len);

rydb_error_t *rydb_error(const rydb_t *db);
int rydb_error_print(const rydb_t *db);
int rydb_error_fprint(const rydb_t *db, FILE *file);
int rydb_error_snprint(const rydb_t *db, char *buf, size_t buflen);
void rydb_error_clear(rydb_t *db);

int rydb_close(rydb_t *db); //also free()s db


#endif //_RYDB_H
