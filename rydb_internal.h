#ifndef _RYDB_INTERNAL_H
#define _RYDB_INTERNAL_H

#include "rydb.h"

int rydb_file_open_index(rydb_t *db, int index_n);
int rydb_file_open_index_data(rydb_t *db, int index_n);
void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...);


#endif //RYDB_INTERNAL_H
