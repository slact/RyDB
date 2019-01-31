#include "rydb.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>

int main(void) {
  rydb_t *db = rydb_new();
  rydb_config_row(db, 50, 10);
  rydb_config_revision(db, 0);
  rydb_config_add_row_link(db, "next", "prev");
  rydb_config_add_row_link(db, "src", "dst");
  rydb_config_add_index_hashtable(db, "zebra", 1, 4, 0, NULL);
  rydb_config_add_index_hashtable(db, "beepus", 4, 11, 1, NULL);
  rydb_config_add_index_hashtable(db, "anthill", 45, 4, 0, NULL);
  if(rydb_open(db, "./" ,"foo")) {
    printf("opened ok\n");
    rydb_close(db);
    return 1;
  }
  else {
    rydb_error_print(db);
    rydb_close(db);
    return 0;
  }
}
