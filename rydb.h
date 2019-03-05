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

typedef struct {
  rydb_file_t          index;
  rydb_file_t          map;
  rydb_config_index_t *config;
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
  RYDB_ERROR_WRONG_ENDIANNESS     = 13,
  RYDB_ERROR_TRANSACTION_ACTIVE   = 14,
  RYDB_ERROR_TRANSACTION_INACTIVE = 15,
  RYDB_ERROR_TRANSACTION_FAILED   = 16,
  RYDB_ERROR_TRANSACTION_INCOMPLETE=17,
  RYDB_ERROR_DATA_TOO_LARGE       = 18,
  RYDB_ERROR_ROWNUM_OUT_OF_RANGE  = 19,
  RYDB_ERROR_DATABASE_CLOSED      = 20,
  RYDB_ERROR_DATABASE_OPEN        = 21,
  RYDB_ERROR_NOT_UNIQUE           = 22
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
  char               *index_scratch;
  uint8_t             unique_index_count;
  rydb_index_t      **unique_index;
  uint8_t             lock_state;
  struct {
    rydb_rownum_t       future_data_rownum;
    unsigned            active:1;
  }                   transaction;
  struct {
    void              (*function)(rydb_t *db, rydb_error_t *err, void *pd);
    void               *privdata;
  }                   error_handler;
  rydb_error_t        error;
  
};// rydb_t


int rydb_global_config_allocator(rydb_allocator_t *);
rydb_t *rydb_new(void);

int rydb_config_row(rydb_t *db, unsigned row_len, unsigned id_len);
int rydb_config_revision(rydb_t *db, unsigned revision);
int rydb_config_add_row_link(rydb_t *db, const char *link_name, const char *reverse_link_name);
int rydb_config_add_index_hashtable(rydb_t *db, const char *name, unsigned start, unsigned len, uint8_t flags, rydb_config_index_hashtable_t *advanced_config);

int rydb_set_error_handler(rydb_t *db, void (*fn)(rydb_t *, rydb_error_t *, void *), void *pd);

int rydb_open(rydb_t *db, const char *path, const char *name);


rydb_row_t *rydb_row_find(rydb_t *db, const char *id); //return 1 if found, 0 if not found

int rydb_row_safe_to_write_directly_check(rydb_t *db, rydb_row_t *row, uint16_t start, uint16_t len);
int rydb_row_insert(rydb_t *db, const char *data, uint16_t len);
int rydb_row_insert_str(rydb_t *db, const char *data);
int rydb_row_delete(rydb_t *db, rydb_rownum_t rownum);
int rydb_row_update(rydb_t *db, rydb_rownum_t rownum, const char *data, uint16_t start, uint16_t len);
int rydb_row_swap(rydb_t *db, rydb_rownum_t rownum1, rydb_rownum_t rownum2);

rydb_error_t *rydb_error(const rydb_t *db);
int rydb_error_print(const rydb_t *db);
int rydb_error_fprint(const rydb_t *db, FILE *file);
int rydb_error_snprint(const rydb_t *db, char *buf, size_t buflen);
void rydb_error_clear(rydb_t *db);

int rydb_transaction_start(rydb_t *db);
int rydb_transaction_finish(rydb_t *db);
int rydb_transaction_cancel(rydb_t *db);

int rydb_find_row(rydb_t *db, char *val, size_t len, rydb_row_t *result);
int rydb_find_row_str(rydb_t *db, char *str, rydb_row_t *result);

int rydb_close(rydb_t *db); //also free()s db
int rydb_delete(rydb_t *db); //deletes all files in an open db


#endif //_RYDB_H
