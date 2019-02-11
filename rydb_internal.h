#ifndef _RYDB_INTERNAL_H
#define _RYDB_INTERNAL_H

#include "rydb.h"

//thanks, Nginx
#define RY_ALIGNMENT   sizeof(unsigned long)    /* platform word */

//works only for a = 2^n
#define ry_align(d, a)     (((d) + (a - 1)) & ~(a - 1))
#define ry_align_ptr(p, a)                                                   \
    (u_char *) (((uintptr_t) (p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))


#define RYDB_DATA_START_OFFSET ry_align(offsetof(rydb_stored_row_t, data), 8)
    
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
#define RYDB_EACH_TX_ROW(db, cur) for(rydb_stored_row_t *cur = db->data_next_row; cur < db->tx_next_row; cur = rydb_row_next(cur, db->stored_row_size, 1))
#define RYDB_REVERSE_EACH_TX_ROW(db, cur) for(rydb_stored_row_t *cur = rydb_row_next(db->tx_next_row, db->stored_row_size, -1); cur >= db->data_next_row; cur = rydb_row_next(cur, db->stored_row_size, -1))

int rydb_file_open(rydb_t *db, const char *what, rydb_file_t *f);
int rydb_file_open_index(rydb_t *db, int index_n);
int rydb_file_open_index_data(rydb_t *db, int index_n);

int rydb_file_close(rydb_t *db, rydb_file_t *f);
int rydb_file_close_index(rydb_t *db, off_t index_n);
int rydb_file_close_data(rydb_t *db, off_t index_n);

int rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz);
int rydb_file_ensure_writable_address(rydb_t *db, rydb_file_t *f, void *addr, size_t sz);
void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...);

int getrandombytes(unsigned char *p, size_t len);

extern rydb_allocator_t rydb_mem;

char *rydb_strdup(const char *str);


//NOTE: this only works correctly if the struct is made without padding
typedef struct {
  rydb_rownum_t rownum;
  uint16_t      start;
  uint16_t      len;
} row_tx_header_t;

#define rydb_row_next(row, sz, n) (void *)((char *)(row) + (sz) * (n))

#endif //RYDB_INTERNAL_H
