#ifndef _RYDB_INTERNAL_H
#define _RYDB_INTERNAL_H
#ifdef __linux__
#define HAVE_MREMAP 1
#define _GNU_SOURCE
#endif

#include "rydb.h"

//thanks, Nginx
#define RY_ALIGNMENT   sizeof(unsigned long)    /* platform word */

//works only for a = 2^n
#define ry_align(d, a)     (((d) + (a - 1)) & ~(a - 1))
#define ry_align_ptr(p, a)                                                   \
    (u_char *) (((uintptr_t) (p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))


#define RYDB_DATA_HEADER_STRING "rydb data"
#define RYDB_ROW_DATA_OFFSET offsetof(rydb_stored_row_t, data)
#define RYDB_DATA_START_OFFSET ry_align(RYDB_ROW_DATA_OFFSET + strlen(RYDB_DATA_HEADER_STRING), 8)

#ifdef RYDB_DEBUG
  extern int rydb_debug_refuse_to_run_transaction_without_commit;
  extern int rydb_debug_disable_urandom;
  extern const char *rydb_debug_hash_key;
  extern int (*rydb_printf)( const char * format, ... );
  extern int (*rydb_fprintf)( FILE * stream, const char * format, ... );
#else
  #define rydb_debug_refuse_to_run_transaction_without_commit 1
  #define rydb_debug_disable_urandom 0
  #define rydb_debug_hash_key 0
  #define rydb_printf printf
  #define rydb_fprintf fprintf
#endif

#ifndef container_of
#ifdef __GNUC__
#define member_type(type, member) __typeof__ (((type *)0)->member)
#else
#define member_type(type, member) const void
#endif

#ifdef __GNUC__
#  define UNUSED(x) x __attribute__((__unused__))
#else
#  define UNUSED(x) x
#endif

    
#define container_of(ptr, type, member) ((type *)( \
    (char *)(member_type(type, member) *){ ptr } - offsetof(type, member)))
#endif

#define RYDB_LOCK_READ    0x01
#define RYDB_LOCK_WRITE   0x02
#define RYDB_LOCK_CLIENT  0x04

//#define RYDB_DEFAULT_MMAP_SIZE 12500000000 //100GB
#define RYDB_DEFAULT_MMAP_SIZE 1024*8
#define RYDB_EACH_CMD_ROW(db, cur) for(rydb_stored_row_t *cur = rydb_rownum_to_row(db, db->data_next_rownum); cur < rydb_rownum_to_row(db, db->cmd_next_rownum); cur = rydb_row_next(cur, db->stored_row_size, 1))
#define RYDB_REVERSE_EACH_CMD_ROW(db, cur) for(rydb_stored_row_t *cur = rydb_rownum_to_row(db, db->cmd_next_rownum - 1); cur >= rydb_rownum_to_row(db, db->data_next_rownum); cur = rydb_row_next(cur, db->stored_row_size, -1))
#define RYDB_EACH_ROW(db, cur) for(rydb_stored_row_t *cur = (void *)db->data.data.start; (char *)cur <= (char *)db->data.file.end - db->stored_row_size; cur = rydb_row_next(cur, db->stored_row_size, 1))

#define REMAP_OFFSET(ptr, offset) (void *)(((char *)ptr) + offset)

bool rydb_file_open(rydb_t *db, const char *what, rydb_file_t *f);
bool rydb_file_open_index(rydb_t *db, rydb_index_t *idx);
bool rydb_file_open_index_map(rydb_t *db, rydb_index_t *idx);

bool rydb_file_close(rydb_t *db, rydb_file_t *f);
bool rydb_file_close_index(rydb_t *db, rydb_index_t *idx);
bool rydb_file_close_data(rydb_t *db, rydb_index_t *idx);

bool rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz, ptrdiff_t *realloc_offset);
bool rydb_file_shrink_to_size(rydb_t *db, rydb_file_t *f, size_t desired_sz);

void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...);

bool rydb_ensure_open(rydb_t *db);
bool rydb_ensure_closed(rydb_t *db, const char *msg);

bool rydb_stored_row_in_range(rydb_t *db, rydb_stored_row_t *row);
bool rydb_rownum_in_data_range(rydb_t *db, rydb_rownum_t rownum); //save an error on failure


bool rydb_transaction_finish_or_continue(rydb_t *db, int finish);
bool rydb_transaction_start_oneshot_or_continue(rydb_t *db, int *transaction_started);
bool rydb_transaction_start_or_continue(rydb_t *db, int *transaction_started);
bool rydb_transaction_run(rydb_t *db, rydb_stored_row_t *last_row_to_run);

void rydb_storedrow_to_row(const rydb_t *db, const rydb_stored_row_t *datarow, rydb_row_t *row);
rydb_stored_row_t *rydb_rownum_to_row(const rydb_t *db, const rydb_rownum_t rownum);
rydb_rownum_t rydb_row_to_rownum(const rydb_t *db, const rydb_stored_row_t *row);
uint_fast16_t rydb_row_data_size(const rydb_t *db, const rydb_row_t *row);
bool rydb_data_append_cmd_rows(rydb_t *db, rydb_row_t *rows, const off_t count);
void rydb_data_update_last_nonempty_data_row(rydb_t *db, rydb_stored_row_t *row_just_emptied);


bool getrandombytes(unsigned char *p, size_t len);
uint64_t crc32(const uint8_t *data, size_t data_len);
uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);

extern rydb_allocator_t rydb_mem;

char *rydb_strdup(const char *str);

const char *rydb_rowtype_str(rydb_row_type_t type);

//indexing stuff
bool rydb_indices_remove_row(rydb_t *db, rydb_stored_row_t *row);
bool rydb_indices_add_row(rydb_t *db, rydb_stored_row_t *row);
bool rydb_indices_update_row(rydb_t *db, rydb_stored_row_t *row, uint_fast8_t step, off_t start, off_t end);
bool rydb_indices_check_unique(rydb_t *db, rydb_rownum_t rownum, const char *data, off_t start, off_t end, uint_fast8_t set_error, void (*callback)(rydb_t *, int , off_t, off_t, rydb_rownum_t, const rydb_stored_row_t *, const char *));
#define RYDB_EACH_INDEX(db, idx) \
  for(rydb_index_t *idx=&db->index[0], *idx_max = &db->index[db->config.index_count]; idx < idx_max; idx++)
#define RYDB_EACH_UNIQUE_INDEX(db, idx) \
  for(rydb_index_t *idx = NULL, **_idx_cur = db->unique_index, **_idx_max = &db->unique_index[db->unique_index_count]; _idx_cur < _idx_max; _idx_cur++) \
    if((idx = *_idx_cur) != NULL)

bool rydb_transaction_data_init(rydb_t *db);
void rydb_transaction_data_reset(rydb_t *db);
void rydb_transaction_data_free(rydb_t *db);
uint_fast8_t rydb_transaction_check_unique(rydb_t *db, const char *val, off_t i);
bool rydb_transaction_unique_add(rydb_t *db, const char *val, off_t i);
bool rydb_transaction_unique_remove(rydb_t *db, const char *val, off_t i);

//NOTE: this only works correctly if the struct is made without padding
typedef struct {
  uint16_t      start;
  uint16_t      len;
} rydb_row_cmd_header_t;

#define rydb_row_next(row, sz, n) (void *)((char *)(row) + (off_t )(sz) * (n))
#define rydb_row_next_rownum(db, row, n) rydb_row_to_rownum(db, rydb_row_next(row, db->stored_row_size, n))

#ifdef RYDB_DEBUG
int rydb_debug_refuse_to_run_transaction_without_commit; //turning this off lets us test more invalid inputs to commands
int rydb_debug_disable_urandom;
int (*rydb_printf)( const char * format, ... );
int (*rydb_fprintf)( FILE * stream, const char * format, ... );
const char*rydb_debug_hash_key;
#else
#define rydb_debug_refuse_to_run_transaction_without_commit 1
#define rydb_debug_disable_urandom 0
#define rydb_printf printf
#define rydb_fprintf fprintf
#define rydb_debug_hash_key 0
#endif

#define rydb_ll_push(type, head, cur, prev, next) do { \
  type *__head = head; \
  if(__head) { \
    __head->prev = cur; \
    (cur)->next = __head; \
    (cur)->prev = NULL; \
    head = (cur); \
  } \
  else { \
    head = (cur); \
    (cur)->prev = NULL; \
    (cur)->next = NULL; \
  } \
} while(0)

#define rydb_ll_remove(type, head, cur, prev, next) do { \
  if(head == cur) { \
    type *__next = cur->next; \
    head = __next; \
    if(__next) __next->prev = NULL; \
  } \
  else { \
    type *__prev = cur->prev; \
    type *__next = cur->next; \
    if(__prev) { \
      __prev->next = __next; \
    } \
    if(__next) { \
      __next->prev = __prev; \
    } \
  } \
} while(0)

#define rydb_index_cursor_attach(index, cur) \
  rydb_ll_push(rydb_cursor_t, index->cursor, cur, prev, next)
  
#define rydb_index_cursor_detach(index, cur) \
  rydb_ll_remove(rydb_cursor_t, index->cursor, cur, prev, next)


const char *rydb_overlay_data_on_row_for_index(const rydb_t *db, char *dst, rydb_rownum_t rownum, const rydb_stored_row_t **cached_row, const char *overlay, off_t ostart, off_t oend, off_t istart, off_t iend);
//debug stuff?
void rydb_print_stored_data(rydb_t *db);

#endif //RYDB_INTERNAL_H
