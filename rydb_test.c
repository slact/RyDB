#include "rydb.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define UNUSED(x) (void)(x)

void print_trace() {
  char pid_buf[30];
  sprintf(pid_buf, "%d", getpid());
  char name_buf[512];
  name_buf[readlink("/proc/self/exe", name_buf, 511)]=0;
  int child_pid = fork();
  if (!child_pid) {           
    dup2(2,1); // redirect output to stderr
    //fprintf(stdout,"stack trace for %s pid=%s\n",name_buf,pid_buf);
    execlp("gdb", "gdb", "--batch", "-n", "-ex", "bt", name_buf, pid_buf, NULL);
    abort(); /* If gdb failed to start */
  } else {
    waitpid(child_pid,NULL,0);
  }
}

static void rydb_error_handler(rydb_t *db, rydb_error_t *err, void *pd) {
  UNUSED(err);
  UNUSED(pd);
  rydb_error_fprint(db, stderr);
  if(geteuid()==0) {//root 
    print_trace();
  }else {
    fprintf(stderr, "  run as root for stack trace\n");
  }
  
  rydb_error_clear(db);
}

int main(void) {
  rydb_t *db = rydb_new();
  rydb_set_error_handler(db, rydb_error_handler, NULL);
  rydb_config_row(db, 50, 10);
  rydb_config_revision(db, 0);
  rydb_config_add_row_link(db, "next", "prev");
  rydb_config_add_row_link(db, "src", "dst");
  rydb_config_add_index_hashtable(db, "zebra", 1, 4, RYDB_INDEX_DEFAULT, NULL);
  rydb_config_add_index_hashtable(db, "beepus", 4, 11, RYDB_INDEX_UNIQUE|4, NULL);
  rydb_config_add_index_hashtable(db, "anthill", 45, 4, RYDB_INDEX_DEFAULT, NULL);
  if(rydb_open(db, "db/" ,"foo")) {
    printf("opened ok\n");
  }
  else {
    printf("that sucked\n");
  }
  
  return !!rydb_error(db);
  
}
