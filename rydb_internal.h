#ifndef _RYDB_INTERNAL_H
#define _RYDB_INTERNAL_H

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


#ifndef container_of
#ifdef __GNUC__
#define member_type(type, member) __typeof__ (((type *)0)->member)
#else
#define member_type(type, member) const void
#endif

#define container_of(ptr, type, member) ((type *)( \
    (char *)(member_type(type, member) *){ ptr } - offsetof(type, member)))
#endif

#define RYDB_LOCK_READ    0x01
#define RYDB_LOCK_WRITE   0x02
#define RYDB_LOCK_CLIENT  0x04

//#define RYDB_DEFAULT_MMAP_SIZE 12500000000 //100GB
#define RYDB_DEFAULT_MMAP_SIZE 1024*8
#define RYDB_EACH_TX_ROW(db, cur) for(rydb_stored_row_t *cur = db->data_next_row; cur < db->cmd_next_row; cur = rydb_row_next(cur, db->stored_row_size, 1))
#define RYDB_REVERSE_EACH_TX_ROW(db, cur) for(rydb_stored_row_t *cur = rydb_row_next(db->cmd_next_row, db->stored_row_size, -1); cur >= db->data_next_row; cur = rydb_row_next(cur, db->stored_row_size, -1))
#define RYDB_EACH_ROW(db, cur) for(rydb_stored_row_t *cur = (void *)db->data.data.start; (char *)cur <= (char *)db->data.file.end - db->stored_row_size; cur = rydb_row_next(cur, db->stored_row_size, 1))

int rydb_file_open(rydb_t *db, const char *what, rydb_file_t *f);
int rydb_file_open_index(rydb_t *db, int index_n);
int rydb_file_open_index_data(rydb_t *db, int index_n);

int rydb_file_close(rydb_t *db, rydb_file_t *f);
int rydb_file_close_index(rydb_t *db, off_t index_n);
int rydb_file_close_data(rydb_t *db, off_t index_n);

int rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz);
int rydb_file_ensure_writable_address(rydb_t *db, rydb_file_t *f, void *addr, size_t sz);
void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...);

int rydb_ensure_open(rydb_t *db);
int rydb_ensure_closed(rydb_t *db, const char *msg);

int rydb_rownum_in_data_range(rydb_t *db, rydb_rownum_t rownum); //save an error on failure

//these always succeed
int rydb_transaction_finish_or_continue(rydb_t *db, int finish);
int rydb_transaction_start_or_continue(rydb_t *db, int *transaction_started);
int rydb_transaction_run(rydb_t *db);

rydb_stored_row_t *rydb_rownum_to_row(const rydb_t *db, const rydb_rownum_t rownum);
rydb_rownum_t rydb_row_to_rownum(const rydb_t *db, const rydb_stored_row_t *row);
rydb_rownum_t rydb_data_next_rownum(rydb_t *db);
uint_fast16_t rydb_row_data_size(const rydb_t *db, const rydb_row_t *row);
int rydb_data_append_cmd_rows(rydb_t *db, rydb_row_t *rows, const off_t count);

int getrandombytes(unsigned char *p, size_t len);

extern rydb_allocator_t rydb_mem;

char *rydb_strdup(const char *str);

const char *rydb_rowtype_str(rydb_row_type_t type);


//NOTE: this only works correctly if the struct is made without padding
typedef struct {
  uint16_t      start;
  uint16_t      len;
} rydb_row_cmd_header_t;

#define rydb_row_next(row, sz, n) (void *)((char *)(row) + (sz) * (n))


//debug stuff?
void rydb_print_stored_data(rydb_t *db);

#endif //RYDB_INTERNAL_H
