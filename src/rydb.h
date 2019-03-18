#ifndef _RYDB_H
#define _RYDB_H
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdbool.h>
#include "rbtree.h"

#define RYDB_FORMAT_VERSION 1

typedef uint32_t rydb_rownum_t;
#define RYDB_ROWNUM_MAX  ((rydb_rownum_t ) -100)
#define RYDB_ROWNUM_NULL ((rydb_rownum_t ) 0)
#define RYDB_ROWNUM_INVALID ((rydb_rownum_t ) -1)
#define RYDB_ROWNUM_PREV  ((rydb_rownum_t ) -3)
#define RYDB_ROWNUM_NEXT  ((rydb_rownum_t ) -2)

#define RYDB_ROW_LINK_PAIRS_MAX 5
#define rydb_link_bitmap_t      uint_fast8_t


#define RYDB_NAME_MAX_LEN 64
#define RYDB_INDICES_MAX 32
#define RYDB_REVISION_MAX UINT16_MAX
#define RYDB_ROW_LEN_MAX UINT16_MAX

#define RYDB_INDEX_DEFAULT  0x0
#define RYDB_INDEX_UNIQUE   0x1

typedef struct rydb_stored_row_s {
  uint8_t     reserved1;
  uint8_t     reserved2;
  uint8_t     reserved3;
  uint8_t     type;
  uint32_t    target_rownum; //used in transactions, not so much in data
  char        data[]; //cool c99 bro
} rydb_stored_row_t;

//each tx must be idempotent!
typedef enum {
  RYDB_ROW_EMPTY        ='\00',
  RYDB_ROW_DATA         ='=',
  RYDB_ROW_CMD_SET      ='@', // [rownum]
  RYDB_ROW_CMD_UPDATE   ='^', // [rownum], uint16_t start, uint16_t len, data
  //when uint16t*2+data > row_len 
  RYDB_ROW_CMD_UPDATE1  ='(', // [rownum], uint16_t start, uint16_t len
  RYDB_ROW_CMD_UPDATE2  =')', //update data
  RYDB_ROW_CMD_DELETE   ='x', // [rownum]
  //RYDB_ROW_CMD_MOVE     ='m', // [dst_rownum], src_rownum
  RYDB_ROW_CMD_SWAP1    ='<', // [src_rownum]
  RYDB_ROW_CMD_SWAP2    ='>', // [dst_rownum] to be replaced by TX_SET when SWAP1 finishes
  RYDB_ROW_CMD_COMMIT   ='!',
} rydb_row_type_t;

typedef struct {
  rydb_rownum_t   num;
  rydb_row_type_t type;
  const char     *data;
  uint16_t        start;
  uint16_t        len;
  struct {
    rydb_rownum_t      buf[RYDB_ROW_LINK_PAIRS_MAX * 2];
    rydb_link_bitmap_t map;
  }               links;
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

typedef enum {
  RYDB_HASH_INVALID =   0,
  RYDB_HASH_CRC32 =     1,
  RYDB_HASH_NOHASH =    2, //treat the value as if it's already a good hash
  RYDB_HASH_SIPHASH =   3
} rydb_hash_function_t;

typedef struct {
  rydb_hash_function_t hash_function;
  uint8_t              rehash;
  float                load_factor_max;
  //storing the value in the hashtable prevents extra datafile reads at the cost of possibly much larger hashtable entries
  unsigned             store_value: 1;
  unsigned             store_hash:  1; //storing the hash adds 8 bytes per bucket entry
  
  //direct mapping uses closed-address linear probing, ideal for a 1-to-1 unique primary index. <2 reads avg.
  enum {
    RYDB_OPEN_ADDRESSING = 0,
    RYDB_SEPARATE_CHAINING = 1
  }                    collision_resolution;
} rydb_config_index_hashtable_t;

#define RYDB_REHASH_DEFAULT                0x00
#define RYDB_REHASH_MANUAL                (1<<1)
#define RYDB_REHASH_ALL_AT_ONCE           (1<<2)
#define RYDB_REHASH_INCREMENTAL_ON_READ   (1<<3)
#define RYDB_REHASH_INCREMENTAL_ON_WRITE  (1<<4)
#define RYDB_REHASH_INCREMENTAL_ADJACENT  (1<<5)
#define RYDB_REHASH_INCREMENTAL           (RYDB_REHASH_INCREMENTAL_ON_READ | RYDB_REHASH_INCREMENTAL_ON_WRITE | RYDB_REHASH_INCREMENTAL_ADJACENT)

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

struct rydb_cursor_s;

typedef struct {
  rydb_file_t          index;
  rydb_file_t          map;
  rydb_config_index_t *config;
  rydb_index_state_t   state;
  struct rydb_cursor_s *cursor;
} rydb_index_t;

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
  RYDB_ERROR_WRONG_ENDIANNESS     = 13,
  RYDB_ERROR_TRANSACTION_ACTIVE   = 14,
  RYDB_ERROR_TRANSACTION_INACTIVE = 15,
  RYDB_ERROR_TRANSACTION_FAILED   = 16,
  RYDB_ERROR_TRANSACTION_INCOMPLETE=17,
  RYDB_ERROR_DATA_TOO_LARGE       = 18,
  RYDB_ERROR_ROWNUM_OUT_OF_RANGE  = 19,
  RYDB_ERROR_DATABASE_CLOSED      = 20,
  RYDB_ERROR_DATABASE_OPEN        = 21,
  RYDB_ERROR_NOT_UNIQUE           = 22,
  RYDB_ERROR_INDEX_NOT_FOUND      = 23,
  RYDB_ERROR_INDEX_INVALID        = 24,
  RYDB_ERROR_WRONG_INDEX_TYPE     = 25,
  RYDB_ERROR_LINK_NOT_FOUND       = 26,
} rydb_error_code_t;
const char *rydb_error_code_str(rydb_error_code_t code);

typedef enum {
  RYDB_STATE_CLOSED = 0,
  RYDB_STATE_OPEN = 1,
} rydb_state_t;

typedef struct {
  rydb_error_code_t    code;
  int                  errno_val;
  char                 str[RYDB_ERROR_MAX_LEN];
} rydb_error_t;

typedef struct {
  void          *(*malloc)(size_t);
  void          *(*realloc)(void *, size_t);
  void           (*free)(void *);
} rydb_allocator_t;

typedef struct {
  uint32_t revision;
  uint16_t row_len;
  uint16_t id_len; //id is part of the row, starting at row[0]
  uint16_t link_pair_count;
  rydb_config_row_link_t *link;
  uint16_t index_count;
  rydb_config_index_t *index;
  struct {
    uint8_t   value[16];
    unsigned  quality:2;
    unsigned  permanent:1;
  }        hash_key;
} rydb_config_t;

typedef struct rydb_s rydb_t;
struct rydb_s {
  rydb_state_t        state;
  const char         *path;
  const char         *name;
  uint16_t            stored_row_size;
  rydb_rownum_t       data_next_rownum; //row after last for RYDB_ROW_DATA
  rydb_rownum_t       cmd_next_rownum;
  rydb_stored_row_t  *cmd_next_row; //row after last for RYDB_ROW_CMD_*, also the next after the last row (of any type) in the data file
  rydb_file_t         data;
  rydb_file_t         meta;
  rydb_file_t         lock;
  rydb_config_t       config;
  rydb_index_t       *index;
  char               *index_scratch_buffer;
  const char        **index_scratch;
  uint8_t             unique_index_count;
  rydb_index_t      **unique_index;
  uint8_t             lock_state;
  struct {
    rydb_rownum_t       future_data_rownum;
    struct {
      RBTree              *added;
      RBTree              *removed;
    }                   unique_index_constraints;
    uint16_t            command_count;
    unsigned            oneshot:1;
    unsigned            active:1;
  }                   transaction;
  struct {
    void              (*function)(rydb_t *db, rydb_error_t *err, void *pd);
    void               *privdata;
  }                   error_handler;
  rydb_error_t        error;
  
};// rydb_t

typedef struct rydb_cursor_s{
  rydb_t            *db;
  off_t              step;
  enum {
    RYDB_CURSOR_TYPE_NONE = 0,
    RYDB_CURSOR_TYPE_DATA = 1,
    RYDB_CURSOR_TYPE_HASHTABLE = 2,
  }                  type;
  unsigned           finished:1;
  const char        *data;
  size_t             len;
  struct rydb_cursor_s *prev;
  struct rydb_cursor_s *next;
  union {
    struct {
      rydb_index_t     *idx;
      rydb_config_index_t *config;
      rydb_index_type_t type;
      union {
        struct {
          uint64_t        hash;
          uint64_t        bucketnum;
          int_fast8_t     bitlevel;
        }               hashtable;
      }                 typedata;
    }                 index;
    struct {
      rydb_rownum_t     rownum;
    }                 data;
  }                 state;
} rydb_cursor_t;

rydb_error_t *rydb_error(const rydb_t *db);
int rydb_error_print(const rydb_t *db);
int rydb_error_fprint(const rydb_t *db, FILE *file);
int rydb_error_snprint(const rydb_t *db, char *buf, size_t buflen);
void rydb_error_clear(rydb_t *db);

void rydb_global_config_allocator(rydb_allocator_t *);
rydb_t *rydb_new(void);

bool rydb_config_row(rydb_t *db, unsigned row_len, unsigned id_len);
bool rydb_config_revision(rydb_t *db, unsigned revision);
bool rydb_config_add_row_link(rydb_t *db, const char *link_name, const char *reverse_link_name);
bool rydb_config_add_index_hashtable(rydb_t *db, const char *name, unsigned start, unsigned len, uint8_t flags, rydb_config_index_hashtable_t *advanced_config);

bool rydb_set_error_handler(rydb_t *db, void (*fn)(rydb_t *, rydb_error_t *, void *), void *pd);

bool rydb_open(rydb_t *db, const char *path, const char *name);

bool rydb_insert(rydb_t *db, const char *data, uint16_t len);
bool rydb_insert_str(rydb_t *db, const char *data);
bool rydb_delete_rownum(rydb_t *db, rydb_rownum_t rownum);
bool rydb_update_rownum(rydb_t *db, rydb_rownum_t rownum, const char *data, uint16_t start, uint16_t len);
bool rydb_swap_rownum(rydb_t *db, rydb_rownum_t rownum1, rydb_rownum_t rownum2);

bool rydb_transaction_start(rydb_t *db);
bool rydb_transaction_finish(rydb_t *db);
bool rydb_transaction_cancel(rydb_t *db);

void rydb_row_init(rydb_row_t *row); //clears the row

//row by rownum
bool rydb_find_row_at(rydb_t *db, rydb_rownum_t rownum, rydb_row_t *row);

//find by primary index
bool rydb_find_row(rydb_t *db, const char *val, size_t len, rydb_row_t *result);
bool rydb_find_row_str(rydb_t *db, const char *str, rydb_row_t *result);
bool rydb_find_rows(rydb_t *db, const char *val, size_t len, rydb_cursor_t *cur);
bool rydb_find_rows_str(rydb_t *db, const char *str, rydb_cursor_t *cur);
//find by named index
bool rydb_index_find_row(rydb_t *db, const char *index_name, const char *val, size_t len, rydb_row_t *result);
bool rydb_index_find_row_str(rydb_t *db, const char *index_name, const char *str, rydb_row_t *result);
bool rydb_index_find_rows(rydb_t *db, const char *index_name, const char *val, size_t len, rydb_cursor_t *cur);
bool rydb_index_find_rows_str(rydb_t *db, const char *index_name, const char *str, rydb_cursor_t *cur);

//cursor stuff
bool rydb_cursor_next(rydb_cursor_t *cur, rydb_row_t *row);
void rydb_cursor_done(rydb_cursor_t *cur);

//all rows
bool rydb_rows(rydb_t *db, rydb_cursor_t *cur);

//row links
bool rydb_row_set_link(rydb_t *db, rydb_row_t *row, const char *link_name, rydb_row_t *linked_row);
bool rydb_row_set_link_rownum(rydb_t *db, rydb_row_t *row, const char *link_name, rydb_rownum_t linked_rownum);
bool rydb_row_get_link(rydb_t *db, const rydb_row_t *row, const char *link_name, rydb_row_t *linked_row);
bool rydb_row_get_link_rownum(rydb_t *db, rydb_rownum_t rownum, const char *link_name, rydb_rownum_t *linked_rownum);

//index-specific stuff
bool rydb_index_rehash(rydb_t *db, const char *index_name);

bool rydb_close(rydb_t *db); //also free()s db
bool rydb_delete(rydb_t *db); //deletes all files in an open db
bool rydb_force_unlock(rydb_t *db);


#endif //_RYDB_H
