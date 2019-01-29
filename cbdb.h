#include <inttypes.h>
#include <limits.h>
#include <stddef.h>

#define CBDB_FORMAT_VERSION 0

typedef uint32_t cbdb_rownum_t;
#define CBDB_ROWNUM_MAX  ((cbdb_rownum_t ) -1)
#define CBDB_ROWNUM_NULL ((cbdb_rownum_t ) 0)

#define CBDB_ROW_LINKS_MAX 4
#define CBDB_INDICES_MAX 32

typedef struct {
  char           *str;
  uint16_t        len;
} cbdb_str_t;

typedef struct {
  cbdb_rownum_t   n;
  cbdb_str_t      id;
  cbdb_str_t      data;
  cbdb_rownum_t   link[CBDB_ROW_LINKS_MAX];
} cbdb_row_t;

typedef struct {
  int      fd;
  void    *start; //first valid mmapped address, also the first byte of the file
  void    *fist; //lfirst data byte (may not be first byte of file due to headers)
  void    *last; //last data byte
  void    *end; //last valid mmapped address
} cbdb_mmap_t;

typedef enum {
  CBDB_INDEX_HASHTABLE=1,
  CBDB_INDEX_BTREE=2
} cbdb_index_type_t;

#define CBDB_INDEX_ID UINT16_MAX

typedef struct {
  char              *name;
  cbdb_index_type_t  type;
  uint16_t           start; // start of indexable value in data. set to CBDB_INDEX_ID to index from start of id
  uint16_t           len; //length of indexable data
} cbdb_config_index_t;

typedef struct {
  cbdb_config_index_t  config;
  cbdb_mmap_t          data;
} cbdb_index_t;

#define CBDB_ERROR_MAX_LEN 1024
typedef enum {
  CBDB_NO_ERROR             = 0,
  CBDB_ERROR_UNSPECIFIED    = 1,
  CBDB_ERROR_NOMEMORY       = 2,
  CBDB_ERROR_FILE_NOT_FOUND = 3,
  CBDB_ERROR_FILE_EXISTS    = 4,
  CBDB_ERROR_LOCK_FAILED    = 5,
  CBDB_ERROR_FILE_ACCESS    = 6,
  CBDB_ERROR_FILE_INVALID   = 7,
  CBDB_ERROR_CONFIG_MISMATCH= 8,
  CBDB_ERROR_REVISION_MISMATCH= 9,
} cbdb_error_code_t;

typedef struct {
  cbdb_error_code_t    code;
  char                *str;
  int                  errno_val;
} cbdb_error_t;

typedef struct {
  uint32_t revision;
  uint16_t id_len;
  uint16_t data_len;
  uint16_t index_count;
} cbdb_config_t;

typedef struct {
  char           *path;
  char           *name;
  cbdb_mmap_t     data;
  cbdb_mmap_t     meta;
  cbdb_config_t   config;
  cbdb_index_t   *index;
  struct {
    char           *id;
    char           *data;
    char           *error;
  }               buffer;
  cbdb_error_t    error;
} cbdb_t;



cbdb_t *cbdb_open(char *path, char *name, cbdb_config_t *cf, cbdb_config_index_t *index, cbdb_error_t *err);
void cbdb_close(cbdb_t *cbdb);

int cbdb_insert(cbdb_t *cbdb, cbdb_str_t *id, cbdb_str_t *data);
int cbdb_insert_row(cbdb_t *cbdb, cbdb_row_t *row); //id and data should be pre-filled
int cbdb_find(cbdb_t *cbdb, cbdb_str_t *id); //return 1 if found, 0 if not found
int cbdb_find_row(cbdb_t *cbdb, cbdb_row_t *row); //id should be pre-filled
