#ifndef _RYDB_INTERNAL_H
#define _RYDB_INTERNAL_H

#include "rydb.h"

//thanks, Nginx
#define RY_ALIGNMENT   sizeof(unsigned long)    /* platform word */

//works only for a = 2^n
#define ry_align(d, a)     (((d) + (a - 1)) & ~(a - 1))
#define ry_align_ptr(p, a)                                                   \
    (u_char *) (((uintptr_t) (p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))

#define RYDB_DATAROW_TYPE_ROW     'R'
#define RYDB_DATAROW_TYPE_FOOTER  'F'


int rydb_file_open_index(rydb_t *db, int index_n);
int rydb_file_open_index_data(rydb_t *db, int index_n);
int rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz);
int rydb_file_ensure_writable_address(rydb_t *db, rydb_file_t *f, void *addr, size_t sz);
void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...);

#endif //RYDB_INTERNAL_H
