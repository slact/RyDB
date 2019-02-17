#include "rydb_internal.h"
#include "rydb_hashtable.h"
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#include <signal.h>
#include <assert.h>

#if defined _WIN32 || defined __CYGWIN__
#define PATH_SLASH_CHAR '\\'
#define PATH_SLASH "\\"
#else
#define PATH_SLASH_CHAR '/'
#define PATH_SLASH "/"
#endif

#ifdef RYDB_DEBUG
int rydb_debug_refuse_to_run_transaction_without_commit = 1; //turning this off lets us test more invalid inputs to commands
int rydb_debug_disable_urandom = 0;
int (*rydb_printf)( const char * format, ... ) = printf;
int (*rydb_fprintf)( FILE * stream, const char * format, ... ) = fprintf;

#endif

rydb_allocator_t rydb_mem = {
  malloc,
  realloc,
  free
};


int rydb_global_config_allocator(rydb_allocator_t *mem) {
  if(mem) {
    rydb_mem = *mem;
  }
  else {
    rydb_mem.malloc = malloc;
    rydb_mem.realloc = realloc;
    rydb_mem.free = free;
  }
  return 1;
}

char *rydb_strdup(const char *str){
  size_t len = strlen(str)+1;
  char *cpy = rydb_mem.malloc(len);
  if(!cpy) {
    return NULL;
  }
  memcpy(cpy, str, len);
  return cpy;
}

static int rydb_index_type_valid(rydb_index_type_t index_type);
static int rydb_find_index_num(const rydb_t *db, const char *name);

static int is_little_endian(void) {
  volatile union {
    uint8_t  c[4];
    uint32_t i;
  } u;
  u.i = 0x01020304;
  return u.c[0] == 0x04;
}

static int is_alphanumeric(const char *str) {
  return strspn(str, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") == strlen(str);
}

const char *rydb_error_code_str(rydb_error_code_t code) {
  switch(code) {
  case RYDB_NO_ERROR:
    return "RYDB_NO_ERROR";
  case RYDB_ERROR_UNSPECIFIED:
    return "RYDB_ERROR_UNSPECIFIED";
  case RYDB_ERROR_NOMEMORY:
    return "RYDB_ERROR_NOMEMORY";
  case RYDB_ERROR_FILE_NOT_FOUND:
    return "RYDB_ERROR_FILE_NOT_FOUND";
  case RYDB_ERROR_FILE_EXISTS:
    return "RYDB_ERROR_FILE_EXISTS";
  case RYDB_ERROR_LOCK_FAILED:
    return "RYDB_ERROR_LOCK_FAILED";
  case RYDB_ERROR_FILE_ACCESS:
    return "RYDB_ERROR_FILE_ACCESS";
  case RYDB_ERROR_FILE_INVALID:
    return "RYDB_ERROR_FILE_INVALID";
  case RYDB_ERROR_FILE_SIZE:
    return "RYDB_ERROR_FILE_SIZE";
  case RYDB_ERROR_CONFIG_MISMATCH:
    return "RYDB_ERROR_CONFIG_MISMATCH";
  case RYDB_ERROR_VERSION_MISMATCH:
    return "RYDB_ERROR_VERSION_MISMATCH";
  case RYDB_ERROR_REVISION_MISMATCH:
    return "RYDB_ERROR_REVISION_MISMATCH";
  case RYDB_ERROR_BAD_CONFIG:
    return "RYDB_ERROR_BAD_CONFIG";
  case RYDB_ERROR_WRONG_ENDIANNESS:
    return "RYDB_ERROR_WRONG_ENDIANNESS";
  case RYDB_ERROR_TRANSACTION_ACTIVE:
    return "RYDB_ERROR_TRANSACTION_ACTIVE";
  case RYDB_ERROR_TRANSACTION_INACTIVE:
    return "RYDB_ERROR_TRANSACTION_INACTIVE";
  case RYDB_ERROR_TRANSACTION_FAILED:
    return "RYDB_ERROR_TRANSACTION_FAILED";
  case RYDB_ERROR_TRANSACTION_INCOMPLETE:
    return "RYDB_ERROR_TRANSACTION_INCOMPLETE";
  case RYDB_ERROR_DATA_TOO_LARGE:
    return "RYDB_ERROR_DATA_TOO_LARGE";
  case RYDB_ERROR_ROWNUM_OUT_OF_RANGE:
    return "RYDB_ERROR_ROWNUM_OUT_OF_RANGE";
  case RYDB_ERROR_DATABASE_CLOSED:
    return "RYDB_ERROR_DATABASE_CLOSED";
  case RYDB_ERROR_DATABASE_OPEN:
    return "RYDB_ERROR_DATABASE_OPEN";
  }
  return "???";
}

const char *rydb_rowtype_str(rydb_row_type_t type) {
  switch(type) {
    case RYDB_ROW_EMPTY: return "RYDB_ROW_EMPTY";
    case RYDB_ROW_DATA: return "RYDB_ROW_DATA";
    case RYDB_ROW_CMD_SET: return "RYDB_ROW_CMD_SET";
    case RYDB_ROW_CMD_UPDATE: return "RYDB_ROW_CMD_UPDATE";
    case RYDB_ROW_CMD_UPDATE1: return "RYDB_ROW_CMD_UPDATE1";
    case RYDB_ROW_CMD_UPDATE2: return "RYDB_ROW_CMD_UPDATE2";
    case RYDB_ROW_CMD_DELETE: return "RYDB_ROW_CMD_DELETE";
    case RYDB_ROW_CMD_SWAP1: return "RYDB_ROW_CMD_SWAP1";
    case RYDB_ROW_CMD_SWAP2: return "RYDB_ROW_CMD_SWAP2";
    case RYDB_ROW_CMD_COMMIT: return "RYDB_ROW_CMD_COMMIT";
  }
  return "???";
}



#define RETURN_ERROR_PRINTF(err, func, ...) \
  if(err->errno_val != 0 && ((err->code >= RYDB_ERROR_FILE_NOT_FOUND && err->code <= RYDB_ERROR_FILE_ACCESS) || err->code == RYDB_ERROR_NOMEMORY)) {            \
    return func(__VA_ARGS__ "%s [%d]: %s, errno [%d]: %s\n", rydb_error_code_str(err->code), err->code, err->str, err->errno_val, strerror(err->errno_val));\
  } \
  return func(__VA_ARGS__ "%s [%d]: %s\n", rydb_error_code_str(err->code), err->code, err->str)

int rydb_error_print(const rydb_t *db) {
  const rydb_error_t *err = &db->error;
  RETURN_ERROR_PRINTF(err, rydb_printf, "");
}

int rydb_error_fprint(const rydb_t *db, FILE *file) {
  const rydb_error_t *err = &db->error;
  RETURN_ERROR_PRINTF(err, rydb_fprintf, file, "");
}
int rydb_error_snprint(const rydb_t *db, char *buf, size_t buflen) {
  const rydb_error_t *err = &db->error;
  RETURN_ERROR_PRINTF(err, snprintf, buf, buflen, "");
}

rydb_error_t *rydb_error(const rydb_t *db) {
  if(db->error.code != RYDB_NO_ERROR) {
    return (rydb_error_t *)&db->error;
  }
  return NULL;
}

void rydb_error_clear(rydb_t *db) {
  db->error.code = RYDB_NO_ERROR;
  db->error.errno_val = 0;
  db->error.str[0] = '\00';
}

int rydb_set_error_handler(rydb_t *db, void (*fn)(rydb_t *, rydb_error_t *, void *), void *pd) {
  db->error_handler.function = fn;
  db->error_handler.privdata = pd;
  return 1;
}

void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...) {
  va_list ap;
  va_start(ap, err_fmt);
  vsnprintf(db->error.str, RYDB_ERROR_MAX_LEN - 1, err_fmt, ap);
  db->error.code = code;
  db->error.errno_val = errno;
  va_end(ap);
  if(db->error_handler.function) {
    db->error_handler.function(db, &db->error, db->error_handler.privdata);
  }
}

rydb_t *rydb_new(void) {
  rydb_t *db = rydb_mem.malloc(sizeof(*db));
  if(!db) {
    return NULL;
  }
  memset(db, '\00', sizeof(*db));
  db->data.fd = -1;
  db->meta.fd = -1;
  db->lock.fd = -1;
  return db;
}

int rydb_config_row(rydb_t *db, unsigned row_len, unsigned id_len) {
  if(!rydb_ensure_closed(db, "and cannot be configured")) {
    return 0;
  }
  if(row_len > RYDB_ROW_LEN_MAX) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row length %u cannot exceed %"PRIu16, row_len, RYDB_ROW_LEN_MAX);
    return 0;
  }
  if(id_len > row_len) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row id length %u cannot exceed row length %u", id_len, row_len);
    return 0;
  }
  if(row_len == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row length cannot be 0");
    return 0;
  }
  db->config.row_len = row_len;
  db->config.id_len = id_len;
  return 1;
}

int rydb_config_revision(rydb_t *db, unsigned revision) {
  if(!rydb_ensure_closed(db, "and cannot be configured")) {
    return 0;
  }
  if(revision > RYDB_REVISION_MAX) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Revision number cannot exceed %"PRIu64, RYDB_REVISION_MAX);
    return 0;
  }
  db->config.revision = revision;
  return 1;
}

static inline int row_link_config_compare(const void *v1, const void *v2) {
  const rydb_config_row_link_t *idx1 = v1;
  const rydb_config_row_link_t *idx2 = v2;
  return strcmp(idx1->next, idx2->next);
}

static int rydb_find_row_link_num(rydb_t *db, const char *next_name) {
  rydb_config_row_link_t match = {.next = next_name };;
  rydb_config_row_link_t *start = db->config.link, *found;
  if(!start) {
    return -1;
  }
  found = bsearch(&match, start, db->config.link_pair_count * 2, sizeof(*start), row_link_config_compare);
  if(!found) {
    return -1;
  }
  return found - start;
}

int rydb_config_add_row_link(rydb_t *db, const char *link_name, const char *reverse_link_name) {
  if(!rydb_ensure_closed(db, "and cannot be configured")) {
    return 0;
  }
  if(db->config.link_pair_count >= RYDB_ROW_LINK_PAIRS_MAX) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Cannot exceed %i row-link pairs per database.", RYDB_ROW_LINK_PAIRS_MAX);
    return 0;
  }
  if(strlen(link_name) == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid row-link name of length 0.");
    return 0;
  }
  if(strlen(reverse_link_name) == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid reverse row-link name of length 0.");
    return 0;
  }
  if(strlen(link_name) > RYDB_NAME_MAX_LEN) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link name is too long, must be at most %i", RYDB_NAME_MAX_LEN);
    return 0;
  }
  if(strlen(reverse_link_name) > RYDB_NAME_MAX_LEN) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Reverse row-link name is too long, must be at most %i", RYDB_NAME_MAX_LEN);
    return 0;
  }
  if(!is_alphanumeric(link_name)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid row-link name \"%s\", must be alphanumeric or underscores.", link_name);
    return 0;
  }
  if(!is_alphanumeric(reverse_link_name)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid reverse row-link name \"%s\", must be alphanumeric or underscores.", reverse_link_name);
    return 0;
  }
  if(strcmp(link_name, reverse_link_name) == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link and reverse row-link cannot be the same.");
    return 0;
  }
  
  if(rydb_find_row_link_num(db, link_name) != -1) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link with name \"%s\" already exists.", link_name);
    return 0;
  }
  if(rydb_find_row_link_num(db, reverse_link_name) != -1) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link with name \"%s\" already exists.", reverse_link_name);
    return 0;
  }
  
  char *nextname = rydb_strdup(link_name);
  char*prevname = rydb_strdup(reverse_link_name);
  if(!nextname || !prevname) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for row-link names");
    if(nextname) rydb_mem.free(nextname);
    if(prevname) rydb_mem.free(prevname);
    return 0;
  }
  
  rydb_config_row_link_t *links;
  if(db->config.link_pair_count == 0) {
    links = rydb_mem.malloc(sizeof(*db->config.link) * 2);
  }
  else {
    links = rydb_mem.realloc(db->config.link, sizeof(*db->config.link) * (db->config.link_pair_count + 1) * 2);
  }
  if(links) {
    db->config.link = links;
  }
  else {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for row-link");
    if(nextname) rydb_mem.free(nextname);
    if(prevname) rydb_mem.free(prevname);
    return 0;
  }
  rydb_config_row_link_t *link, *link_inverse;
  off_t offset = db->config.link_pair_count * 2;
  link = &db->config.link[offset];
  link_inverse = &db->config.link[offset + 1];
  
  link->inverse = 0;
  link->next = nextname;
  link->prev = prevname;
  
  link_inverse->inverse = 1;
  link_inverse->next = link->prev;
  link_inverse->prev = link->next;
  
  db->config.link_pair_count ++;
  qsort(db->config.link, db->config.link_pair_count * 2, sizeof(*db->config.link), row_link_config_compare);
  
  return 1;
}

static inline int index_config_compare(const void *v1, const void *v2) {
  const rydb_config_index_t *idx1 = v1;
  const rydb_config_index_t *idx2 = v2;
  return strcmp(idx1->name, idx2->name);
}

static int rydb_config_add_index(rydb_t *db, rydb_config_index_t *idx) {
  int primary = 0;
  if(strcmp(idx->name, "primary") == 0 || rydb_find_index_num(db, "primary") != -1) {
    primary = 1;
  }
  if(strcmp(idx->name, "primary") == 0 && !(idx->flags & RYDB_INDEX_UNIQUE)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Primary index must have RYDB_INDEX_UNIQUE flag");
    return 0;
  }
  if(db->config.index_count >= RYDB_INDICES_MAX - 1 + primary) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Cannot exceed %i indices for this database.", RYDB_INDICES_MAX - 1 + primary);
    return 0;
  }
  if(strlen(idx->name) > RYDB_NAME_MAX_LEN) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index name \"%s\" too long, must be at most %i characters", idx->name, RYDB_NAME_MAX_LEN);
    return 0;
  }
  if(!is_alphanumeric(idx->name)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index name \"%s\" invalid: must consist of only ASCII alphanumeric characters and underscores", idx->name);
    return 0;
  }
  if(!rydb_index_type_valid(idx->type)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" type is invalid", idx->name);
    return 0;
  }
  if(idx->start > db->config.row_len) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" is out of bounds: row length is %"PRIu16", but index is set to start at %"PRIu16, idx->name, db->config.row_len, idx->start);
    return 0;
  }
  if(idx->start + idx->len > db->config.row_len) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" is out of bounds: row length is %"PRIu16", but index is set to end at %"PRIu16, idx->name, db->config.row_len, idx->start + idx->len);
    return 0;
  }
  if(rydb_find_index_num(db, idx->name) != -1) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" already exists", idx->name);
    return 0;
  }
  
  //allocation
  char *idxname = rydb_strdup(idx->name);
  if(!idxname) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for index \"%s\" name", idx->name);
    return 0;
  }
  
  rydb_config_index_t *indices;
  if(db->config.index_count == 0) {
    indices = rydb_mem.malloc(sizeof(*db->config.index));
  }
  else {
    indices = rydb_mem.realloc(db->config.index, sizeof(*db->config.index) * (db->config.index_count + 1));
  }
  if(indices) {
    db->config.index = indices;
  } 
  else {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for index \"%s\"", idx->name);
    if(idxname) rydb_mem.free(idxname);
    return 0;
  }
  rydb_config_index_t *new_idx = &db->config.index[db->config.index_count];
  
  *new_idx = *idx;
  new_idx->name = idxname;
  
  db->config.index_count++;
  
  qsort(db->config.index, db->config.index_count, sizeof(*db->config.index), index_config_compare);
  
  return 1;
}

//return array position of index if found, -1 if not found
static int rydb_find_index_num(const rydb_t *db, const char *name) {
  rydb_config_index_t match;
  match.name = name;
  rydb_config_index_t *cf_start = db->config.index, *cf;
  if(!cf_start) {
    return -1;
  }
  cf = bsearch(&match, cf_start, db->config.index_count, sizeof(*cf_start), index_config_compare);
  if(!cf) {
    return -1;
  }
  return cf - cf_start;
}

static int rydb_config_index_check_flags(rydb_t *db, const rydb_config_index_t *idx) {
  if((idx->flags & ~(RYDB_INDEX_UNIQUE)) > 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Unknown flags set for index \"%s\"", idx->name);
    return 0;
  }
  return 1;
}

int rydb_config_add_index_hashtable(rydb_t *db, const char *name, unsigned start, unsigned len, uint8_t flags, rydb_config_index_hashtable_t *advanced_config) {
  rydb_config_index_t idx;
  idx.name = name;
  idx.type = RYDB_INDEX_HASHTABLE;
  idx.start = start;
  idx.len = len;
  idx.flags = flags;
  
  if(!rydb_ensure_closed(db, "and cannot be configured")) {
    return 0;
  }
  
  if(!rydb_config_index_check_flags(db, &idx)) {
    return 0;
  }
  
  if(!rydb_config_index_hashtable_set_config(db, &idx, advanced_config)) {
    return 0;
  }
  
  return rydb_config_add_index(db, &idx);
}


static off_t rydb_filename(const rydb_t *db, const char *what, char *buf, off_t maxlen) {
  return snprintf(buf, maxlen, "%s%srydb.%s%s%s",
                  db->path,
                  strlen(db->path) > 0 ? PATH_SLASH : "",
                  db->name,
                  strlen(db->name) > 0 ? "." : "",
                  what);
}
#define rydb_subfree(pptr) \
if(*pptr) do { \
  rydb_mem.free((void *)*(pptr)); \
  *(pptr) = NULL; \
} while(0)

static void rydb_free(rydb_t *db) {
  rydb_subfree(&db->path);
  rydb_subfree(&db->name);
  for(int i = 0; i < db->config.index_count; i++) {
    if(db->config.index) {
      rydb_subfree(&db->config.index[i].name);
    }
  }
  rydb_subfree(&db->config.index);
  rydb_subfree(&db->index);
  rydb_subfree(&db->unique_index);
  
  if(db->config.link) {
    for(int i = 0; i < db->config.link_pair_count * 2; i++) {
      rydb_subfree(&db->config.link[i].next);
    }
    rydb_subfree(&db->config.link);
  }
  
  rydb_mem.free(db);
}

static int rydb_lock(rydb_t *db, uint8_t lockflags) {
  /*lockfile layout, in bytes
   [0]: global write lock
   [1]: global read lock
   [2]: global client lock
   ...
  */
  volatile int8_t *writelock = (int8_t *)&db->lock.file.start[0];
  //volatile int8_t *readlock = &db->lock.file.start[1];
  //volatile int8_t *clientlock = &db->lock.file.start[2];
  // we only support single-user mode for now
  if(lockflags & RYDB_LOCK_WRITE) {
    if(++(*writelock) > 1) {
      (*writelock)--;
      rydb_set_error(db, RYDB_ERROR_LOCK_FAILED, "Failed to acquire write-lock");
      return 0;
    }
  }
  //don't care about the rest for now
  return 1;
}

static int rydb_unlock(rydb_t *db, uint8_t lockflags) {
  volatile int8_t *writelock = (int8_t *)&db->lock.file.start[0];
  //volatile int8_t *readlock = &db->lock.file.start[1];
  //volatile uint8_t *clientlock = &db->lock.file.start[2];
  // we only support single-user mode for now
  if(db->lock_state & lockflags & RYDB_LOCK_WRITE) {
    *writelock--;
  }
  //don't care about the rest for now
  return 1;
}

int rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz) {
  size_t current_sz = f->file.end - f->file.start;
  if(current_sz < desired_min_sz && ftruncate(f->fd, desired_min_sz) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_SIZE, "Failed to grow file to size %zu", desired_min_sz);
    return 0;
  }
  return 1;
}
int rydb_file_ensure_writable_address(rydb_t *db, rydb_file_t *f, void *addr, size_t sz) {
  char *end = (char *)addr + sz;
  if(end > f->file.end) {
    if(ftruncate(f->fd, (size_t )((char *)end - (char *)f->file.start)) == -1) {
      rydb_set_error(db, RYDB_ERROR_FILE_SIZE, "Failed to grow file to size %zu", (size_t )((char *)end - (char *)f->file.end));
      return 0;
    }
    f->file.end = end;
    f->data.end = end;
  }
  
  return 1;
}
int rydb_file_shrink_to_size(rydb_t *db, rydb_file_t *f, size_t desired_sz) {
  size_t current_sz = f->file.end - f->file.start;
  if(current_sz > desired_sz && ftruncate(f->fd, desired_sz) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_SIZE, "Failed to shrink file to size %zu", desired_sz);
    return 0;
  }
  f->file.end = f->file.start + desired_sz;
  if(f->data.end > f->file.end) {
    f->data.end = f->file.end;
  }
  
  return 1;
}


int rydb_ensure_open(rydb_t *db) {
  if(db->state != RYDB_STATE_OPEN) {
    rydb_set_error(db, RYDB_ERROR_DATABASE_CLOSED, "Database is not open");
    return 0;
  }
  return 1;
}
int rydb_ensure_closed(rydb_t *db, const char *msg) {
  if(db->state != RYDB_STATE_CLOSED) {
    rydb_set_error(db, RYDB_ERROR_DATABASE_OPEN, "Database is open %s", msg ? msg : "");
    return 0;
  }
  return 1;
}

static int rydb_file_getsize(rydb_t *db, int fd, off_t *sz) {
  struct stat st;
  if(fstat(fd, &st)) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to get filesize");
    return 0;
  }
  *sz = st.st_size;
  return 1;
}

int rydb_file_close(rydb_t *db, rydb_file_t *f) {
  int ok = 1;
  if(f->mmap.start && f->mmap.start != MAP_FAILED) {
    if(munmap(f->mmap.start, f->mmap.end - f->mmap.start) == -1) {
      ok = 0;
      rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to munmap file %s", f->path);
    }
  }
  if(f->fp) {
    if(fclose(f->fp) == EOF && ok) {
      //failed to close file
      ok = 0;
      rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to close file pointer for %s", f->path);
    }
    f->fp = NULL;
    //since fp was fdopen()'d, the fd is now also closed
    f->fd = -1;
  }
  if(f->fd != -1) {
    if(close(f->fd) == -1 && ok) {
      ok = 0;
      rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to close file descriptor for %s", f->path);
    }
    f->fd = -1;
  }
  f->mmap.start = NULL;
  f->mmap.end = NULL;
  f->file.start = NULL;
  f->file.end = NULL;
  f->data.start = NULL;
  f->data.end = NULL;
  
  if(f->path) {
    rydb_mem.free((char *)f->path);
    f->path = NULL;
  }
  return ok;
}

int rydb_file_close_index(rydb_t *db, off_t index_n) {
  return rydb_file_close(db, &db->index[index_n].index);
}
int rydb_file_close_data(rydb_t *db, off_t index_n) {
  return rydb_file_close(db, &db->index[index_n].data);
}

int rydb_file_open(rydb_t *db, const char *what, rydb_file_t *f) {
  off_t sz;
  char path[2048];
  rydb_filename(db, what, path, 2048);
  
  if((f->path = rydb_strdup(path)) == NULL) { //useful for debugging
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for file path %s", path);
    return 0;
  }
  
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  if((f->fd = open(path, O_RDWR | O_CREAT, mode)) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to open file %s", path);
    rydb_file_close(db, f);
    return 0;
  }
  
  if((f->fp = fdopen(f->fd, "r+")) == NULL) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to fdopen file %s", path);
    rydb_file_close(db, f);
    return 0;
  }
  
  sz = RYDB_DEFAULT_MMAP_SIZE;
  f->mmap.start = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, f->fd, 0);
  if(f->mmap.start == MAP_FAILED) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to mmap file %s", path);
    rydb_file_close(db, f);
    return 0;
  }
  f->mmap.end = &f->mmap.start[sz]; //last mmapped address
  
  f->file.start = f->mmap.start;
  if(!rydb_file_getsize(db, f->fd, &sz)) {
    rydb_file_close(db, f);
    return 0;
  }
  f->file.end = &f->file.start[sz];
  
  f->data = f->file;
  
  return 1;
}

int rydb_file_open_index(rydb_t *db, int index_n) {
  char index_name[128];
  snprintf(index_name, 128, "index.%s", db->config.index[index_n].name);
  return rydb_file_open(db, index_name, &db->index[index_n].index);
}
int rydb_file_open_index_data(rydb_t *db, int index_n) {
  char index_name[128];
  snprintf(index_name, 128, "index.%s.data", db->config.index[index_n].name);
  return rydb_file_open(db, index_name, &db->index[index_n].data);
}

static int rydb_index_type_valid(rydb_index_type_t index_type) {
  switch(index_type) {
    case RYDB_INDEX_HASHTABLE:
    case RYDB_INDEX_BTREE:
      return 1;
    case RYDB_INDEX_INVALID:
      return 0;
  }
  return 0;
}

static const char *rydb_index_type_str(rydb_index_type_t index_type) {
  switch(index_type) {
    case RYDB_INDEX_HASHTABLE:
      return "hashtable";
    case RYDB_INDEX_BTREE:
      return "B-tree";
    case RYDB_INDEX_INVALID:
      return "invalid";
  }
  return "???";
}

static rydb_index_type_t rydb_index_type(const char *str) {
  if(strcmp(str, "hashtable") == 0) {
    return RYDB_INDEX_HASHTABLE;
  }
  if(strcmp(str, "B-tree") == 0) {
    return RYDB_INDEX_BTREE;
  }
  return RYDB_INDEX_INVALID;
}

static int rydb_meta_save(rydb_t *db) {
  FILE     *fp = db->meta.fp;
  int       rc;
  int       total_written = 0;
  rydb_config_index_t *idxcf;
  
  if(fseek(fp, 0, SEEK_SET) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Failed seeking to start of meta file %s", db->meta.path);
    return 0;
  }
  
  char hash_key_hexstr_buf[33];
  for (unsigned i = 0; i < sizeof(db->config.hash_key.value); i ++) {
    sprintf(&hash_key_hexstr_buf[i*2], "%02x", db->config.hash_key.value[i]);
  }
  hash_key_hexstr_buf[sizeof(db->config.hash_key.value)*2]='\00';
  
  
  const char *fmt =
    "--- #rydb\n"
    "format_revision: %i\n"
    "database_revision: %"PRIu32"\n"
    "storage_info:\n"
    "  endianness: %s\n"
    "  start_offset: %"PRIu16"\n"
    "  row_format:\n"
    "    type_offset: %i\n"
    "    reserved_offset: %i\n"
    "    data_offset: %i\n"
    "  rownum_width: %"PRIu16"\n"
    "hash_key: %s\n"
    "hash_key_quality: %"PRIu8"\n"
    "row_len: %"PRIu16"\n"
    "id_len: %"PRIu16"\n"
    "index_count: %"PRIu16"\n"
    "%s";
  rc = fprintf(fp, fmt, 
               RYDB_FORMAT_VERSION,
               db->config.revision,
               is_little_endian() ? "little" : "big",
               //storage info
               RYDB_DATA_START_OFFSET,
               offsetof(rydb_stored_row_t, type),
               offsetof(rydb_stored_row_t, reserved1),
               RYDB_ROW_DATA_OFFSET,
               (uint16_t)sizeof(rydb_rownum_t),
               hash_key_hexstr_buf,
               (uint8_t )db->config.hash_key.quality,
               db->config.row_len,
               db->config.id_len,
               db->config.index_count,
               db->config.index_count > 0 ? "index:" : ""
  );
  if(rc <= 0) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to meta file %s", db->meta.path);
    return 0;
  }
  total_written += rc;
  
  const char *index_fmt =
    "\n"
    "  - name: %s\n"
    "    type: %s\n"
    "    start: %"PRIu16"\n"
    "    len: %"PRIu16"\n"
    "    unique: %"PRIu16"\n";
    
  for(int i = 0; i < db->config.index_count; i++) {
    idxcf = &db->config.index[i];
    rc = fprintf(fp, index_fmt, idxcf->name, rydb_index_type_str(idxcf->type), idxcf->start, idxcf->len, (uint16_t )(idxcf->flags & RYDB_INDEX_UNIQUE));
    if(rc <= 0) {
      rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to meta file %s", db->meta.path);
      return 0;
    }
    total_written += rc;
    switch(idxcf->type) {
      case RYDB_INDEX_HASHTABLE:
        rc = rydb_meta_save_index_hashtable(db, idxcf, fp);
        break;
      case RYDB_INDEX_BTREE:
      case RYDB_INDEX_INVALID:
        rydb_set_error(db, RYDB_ERROR_UNSPECIFIED, "Unsupported index type");
        return 0;
    }
    total_written += rc;
    if(rc <= 0) {
      return 0;
    }
  }
  
  //now links
  rc = fprintf(fp, "link_pair_count: %"PRIu16"\n%s", db->config.link_pair_count, db->config.link_pair_count > 0 ? "link_pair:\n" : "");
  if(rc <= 0) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to meta file %s", db->meta.path);
    return 0;
  }
  total_written += rc;
  
  for(int i = 0; i < db->config.link_pair_count * 2; i++) {
    if(!db->config.link[i].inverse) {
      rc = fprintf(fp, "  - [ %s , %s ]\n", db->config.link[i].next, db->config.link[i].prev);
      if(rc <= 0) {
        rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to meta file %s", db->meta.path);
        return 0;
      }
      total_written += rc;
    }
  }
  fflush(fp);
  return total_written;
}



#define QUOTE(str) #str
#define EXPAND_AND_QUOTE(str) QUOTE(str)
#define RYDB_NAME_MAX_LEN_STR EXPAND_AND_QUOTE(RYDB_NAME_MAX_LEN)

static int rydb_meta_load(rydb_t *db, rydb_file_t *ryf) {
  FILE     *fp = ryf->fp;
  char      endianness_buf[17];
  char      rowformat_buf[33];
  char      hashkey_buf[35];
  uint8_t   hashkey_quality;
  int       little_endian;
  uint16_t  rydb_format_version, db_revision, start_offset, rownum_width, row_len, id_len, index_count;
  if(fseek(fp, 0, SEEK_SET) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Failed seeking to start of data file");
    return 0;
  }
  
  struct {
    uint16_t  type_off;
    uint16_t  reserved_off;
    uint16_t  data_off;
  } rowformat;
  
  const char *fmt =
    "--- #rydb\n"
    "format_revision: %"SCNu16"\n"
    "database_revision: %"SCNu16"\n"
    "storage_info:\n"
    "  endianness: %15s\n"
    "  start_offset: %"SCNu16"\n"
    "  row_format:\n"
    "    type_offset: %"SCNu16"\n"
    "    %31s %"SCNu16"\n"
    "    data_offset: %"SCNu16"\n"
    "  rownum_width: %"SCNu16"\n"
    "hash_key: %34s\n"
    "hash_key_quality: %"SCNu8"\n"
    "row_len: %"SCNu16"\n"
    "id_len: %"SCNu16"\n"
    "index_count: %"SCNu16"\n";
  int rc = fscanf(fp, fmt,
                  &rydb_format_version,
                  &db_revision,
                  endianness_buf,
                  &start_offset,
                  &rowformat.type_off,
                  rowformat_buf,
                  &rowformat.reserved_off,
                  &rowformat.data_off,
                  &rownum_width,
                  hashkey_buf,
                  &hashkey_quality,
                  &row_len,
                  &id_len,
                  &index_count
  );
  
  if(rc < 11) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Not a RyDB file or is corrupted");
    return 0;
  }
  if(rydb_format_version != RYDB_FORMAT_VERSION) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Format version mismatch, expected %i, loaded %"PRIu16, RYDB_FORMAT_VERSION, rydb_format_version);
    return 0;
  }
  
  if(strcmp("reserved_offset:", rowformat_buf) != 0) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Unknown row format, expected 'reserved_offset', got %s", rowformat_buf);
    return 0;
  }
  
  if(rowformat.type_off != offsetof(rydb_stored_row_t, type) || rowformat.reserved_off != offsetof(rydb_stored_row_t, reserved1) || rowformat.data_off != offsetof(rydb_stored_row_t, data)) {
    //TODO: format conversions
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Row format mismatch");
    return 0;
  }
  
  if(strcmp(endianness_buf, "big") == 0) {
    little_endian = 0;
  }
  else if(strcmp(endianness_buf, "little") == 0) {
    little_endian = 1;
  }
  else {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "File unreadable, unexpected endianness %s", endianness_buf);
    return 0;
  }
  if(is_little_endian() != little_endian) {
    //TODO: convert data to host endianness
    rydb_set_error(db, RYDB_ERROR_WRONG_ENDIANNESS, "File has wrong endianness");
    return 0;
  }
  if(start_offset != RYDB_DATA_START_OFFSET) {
    //TODO: move data to the right offset
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Wrong data offset, expected %i, got %"PRIu16, RYDB_DATA_START_OFFSET, start_offset);
    return 0;
  }
  
  if(rownum_width != sizeof(rydb_rownum_t)) {
    //TODO: convert data to host rownum size
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Rownum is a %" PRIu16"-bit integer, expected %i-bit", rownum_width * 8, sizeof(rydb_rownum_t) * 8);
    return 0;
  }
  
  if(index_count > RYDB_INDICES_MAX) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "File invalid, too many indices defined");
    return 0;
  }
  
  if(strlen(hashkey_buf) != 32) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Invalid hash key length");
    return 0;
  }
  uint8_t hashkey[16];
  for (int i = 15; i >= 0; i--) {
    if(sscanf(&hashkey_buf[i*2], "%"SCNx8, &hashkey[i]) != 1) {
      rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Invalid hash key at [%i]", i*2);
      return 0;
    }
    hashkey_buf[i*2]=' ';
  }
  
  if(hashkey_quality >1) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Invalid hash key quality");
    return 0;
  }
  
  db->config.hash_key.quality = hashkey_quality;
  memcpy(db->config.hash_key.value, hashkey, sizeof(hashkey));
  db->config.hash_key.permanent = 1; //this comes from a saved db. its hashkey can't be changed.
  
  if(!rydb_config_row(db, row_len, id_len)) {
    return 0;
  }
  if(!rydb_config_revision(db, db_revision)) {
    return 0;
  }
  
  if(index_count > 0) {
    const char *index_fmt =
      "\n"
      "  - name: %" RYDB_NAME_MAX_LEN_STR "s\n"
      "    type: %32s\n"
      "    start: %"SCNu16"\n"
      "    len: %"SCNu16"\n"
      "    unique: %"SCNu16"\n";
    
    char                      index_type_buf[33];
    char                      index_name_buf[RYDB_NAME_MAX_LEN+1];
    uint16_t                  index_unique;
    rydb_config_index_t       idx_cf;
    
    if(fscanf(fp, "index:") < 0) {
      rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "index specification is corrupted or invalid");
      return 0;
    }
    for(int i = 0; i < index_count; i++) {
      rc = fscanf(fp, index_fmt, index_name_buf, index_type_buf, &idx_cf.start, &idx_cf.len, &index_unique);
      if(rc < 5) {
        rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "index specification is corrupted or invalid");
        return 0;
      }
      if(index_unique > 1) {
        rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "index \"%s\" uniqueness value is invalid", index_name_buf);
        return 0;
      }
      idx_cf.type = rydb_index_type(index_type_buf);
      idx_cf.name = index_name_buf;
      idx_cf.flags = 0;
      if(index_unique) {
        idx_cf.flags |= RYDB_INDEX_UNIQUE;
      }
      switch(idx_cf.type) {
        case RYDB_INDEX_HASHTABLE:
          if(!rydb_meta_load_index_hashtable(db, &idx_cf, fp)) {
            return 0;
          }
          break;
        case RYDB_INDEX_BTREE:
          rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "index \"%s\" type btree is not supported", index_name_buf);
          return 0;
        case RYDB_INDEX_INVALID:
          rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "index \"%s\" type is invalid", index_name_buf);
          return 0;
      }
      if(!rydb_config_add_index(db, &idx_cf)) {
        return 0;
      }
    }
  }
  
  //now let's do the row links
  uint16_t           linkpairs_count;
  rc = fscanf(fp, "link_pair_count: %"SCNu16"\n", &linkpairs_count);
  if(rc < 1 || linkpairs_count > RYDB_ROW_LINK_PAIRS_MAX) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "link specification is corrupted or invalid");
    return 0;
  }
  if(linkpairs_count > 0) {
    char             link_next_buf[RYDB_NAME_MAX_LEN+1], link_prev_buf[RYDB_NAME_MAX_LEN+1];
    if(fscanf(fp, "link_pair:\n") < 0) {
      rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "link specification is corrupted or invalid");
      return 0;
    }
    for(int i = 0; i < linkpairs_count; i++) {
      rc = fscanf(fp, "  - [ %" RYDB_NAME_MAX_LEN_STR "s , %" RYDB_NAME_MAX_LEN_STR "s ]\n", link_next_buf, link_prev_buf);
      if(rc < 2) {
        rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "link specification is corrupted or invalid");
        return 0;
      }
      if(!rydb_config_add_row_link(db, link_next_buf, link_prev_buf)) {
        return 0;
      }
    }
  }
  
  //ok, that's everything
  return 1;
}

static int rydb_config_match(rydb_t *db, const rydb_t *db2, const char *db_lbl, const char *db2_lbl) {
  //see if the loaded config and the one passed in are the same
  if(db->config.revision != db2->config.revision) {
    rydb_set_error(db, RYDB_ERROR_REVISION_MISMATCH, "Mismatching revision number: %s %"PRIu32", %s %"PRIu32, db_lbl, db->config.revision, db2_lbl, db2->config.revision);
    return 0;
  }
  if(db->config.row_len != db2->config.row_len) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching row length: %s %"PRIu16", %s %"PRIu32, db_lbl, db->config.row_len, db2_lbl, db2->config.row_len);
    return 0;
  }
  if(db->config.id_len != db2->config.id_len) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching id length: %s %"PRIu16", %s %"PRIu16, db_lbl, db->config.id_len, db2_lbl, db2->config.id_len);
    return 0;
  }
  
  // if both dbs have permanent hash keys, check 'em. otherwise don't bother, 
  // one of them hasn't been used yet and can be set to the other's
  if(db->config.hash_key.permanent && db2->config.hash_key.permanent) {
    if(memcmp(db->config.hash_key.value, db2->config.hash_key.value, sizeof(db->config.hash_key.value)) != 0) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching hash keys");
      return 0;
    }
  }
  
  if(db->config.index_count != db2->config.index_count) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching index count: %s %"PRIu16", %s %"PRIu16, db_lbl, db->config.index_count, db2_lbl, db2->config.index_count);
    return 0;
  }
  //compare indices
  for(int i = 0; i < db2->config.index_count; i++) {
    rydb_config_index_t *idx1 = &db->config.index[i];
    rydb_config_index_t *idx2 = &db2->config.index[i];
    if(strcmp(idx1->name, idx2->name) != 0) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching index %i name: expected %s, loaded %s", i, idx1->name, idx2->name);
      return 0;
    }
    if(idx1->type != idx2->type) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching index %i type: expected %s, loaded %s", i, rydb_index_type_str(idx1->type), rydb_index_type_str(idx2->type));
      return 0;
    }
    if(idx1->start != idx2->start) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching index %i start: expected %"PRIu16", loaded %"PRIu16, i, idx1->start, idx2->start);
      return 0;
    }
    if(idx1->len != idx2->len) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching index %i length: expected %"PRIu16", loaded %"PRIu16, i, idx1->len, idx2->len);
      return 0;
    }
  }
  
  //compare row-links
  if(db->config.link_pair_count != db2->config.link_pair_count) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching row-link pair count: %s %"PRIu16", %s %"PRIu16, db_lbl, db->config.link_pair_count, db2_lbl, db2->config.link_pair_count);
    return 0;
  }
  for(int i = 0; i < db2->config.link_pair_count * 2; i++) {
    rydb_config_row_link_t *link1 = &db->config.link[i];
    rydb_config_row_link_t *link2 = &db2->config.link[i];
    if(strcmp(link1->next, link2->next) != 0 || strcmp(link1->prev, link2->prev) != 0) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching row-link pair %i: %s [%s, %s], %s [%s, %s]", i, db_lbl, link1->next, link1->prev, db2_lbl, link2->next, link2->prev);
      return 0;
    }
    if(link1->inverse != link2->inverse) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Mismatching row-link pair %i: %s [%s, %s], %s [%s, %s]", i,
                     db_lbl, link1->inverse ? link1->prev : link1->next, link1->inverse ? link1->next : link1->prev,
                     db2_lbl, link2->inverse ? link2->prev : link2->next, link2->inverse ? link2->next : link2->prev
      );
      return 0;
    }
  }
  
  return 1;
}

static int rydb_data_scan_tail(rydb_t *db) {
  uint16_t stored_row_size = db->stored_row_size;
  rydb_stored_row_t *firstrow = (void *)(db->data.data.start);
  rydb_stored_row_t *last_possible_row = (void *)((char *)firstrow + stored_row_size * ((db->data.file.end - (char *)firstrow)/stored_row_size));
  uint_fast8_t  lastrow_found=0, data_lastrow_found = 0;
  rydb_stored_row_t *last_commit_row = NULL;
  for(rydb_stored_row_t *cur = last_possible_row; cur && cur >= firstrow; cur = rydb_row_next(cur, stored_row_size, -1)) {
    if(!lastrow_found && cur->type != RYDB_ROW_EMPTY) {
      db->cmd_next_row = rydb_row_next(cur, stored_row_size, 1);
      lastrow_found = 1;
    }
    if(!last_commit_row && cur->type == RYDB_ROW_CMD_COMMIT) {
      last_commit_row = cur;
    }
    if(!data_lastrow_found && cur->type == RYDB_ROW_DATA) {
      db->data_next_row = rydb_row_next(cur, stored_row_size, 1);
      data_lastrow_found = 1;
      break;
    }
  }
  if(!lastrow_found) {
    db->cmd_next_row = (void *)db->data.data.start;
  }
  if(!data_lastrow_found) {
    db->data_next_row = (void *)db->data.data.start;
  }
  
  if(last_commit_row) {
    if(!rydb_transaction_run(db, last_commit_row)) {
      return 0;
    }
  }
  //truncate the tx log
  if(!rydb_file_shrink_to_size(db, &db->data, (char *)db->data_next_row - db->data.file.start)) {
    return 0;
  }
  assert(db->data_next_row >= db->data.file.end);
  assert(db->data_next_row >= db->data.data.end);
  return 1;
  
}

static int rydb_data_file_exists(const rydb_t *db) {
  char path[1024];
  rydb_filename(db, "data", path, 1024);
  return access(path, F_OK) != -1;
}

static void rydb_close_nofree(rydb_t *db) {
  rydb_file_close(db, &db->data);
  rydb_file_close(db, &db->meta);
  rydb_file_close(db, &db->lock);
  if(db->index) {
    for(int i = 0; i < db->config.index_count; i++) {
      rydb_file_close(db, &db->index[i].index);
      rydb_file_close(db, &db->index[i].data);
    }
  }
}

static int rydb_open_abort(rydb_t *db) {
  rydb_subfree(&db->path);
  rydb_subfree(&db->name);
  rydb_close_nofree(db);
  rydb_subfree(&db->index);
  db->state = RYDB_STATE_CLOSED;
  return 0;
}


int rydb_open(rydb_t *db, const char *path, const char *name) {
  int           new_db = 0;
  if(!rydb_ensure_closed(db, "and cannot be reopened")) {
    return rydb_open_abort(db);
  }
  
  if((db->path = rydb_strdup(path)) == NULL) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory to open RyDB");
    return rydb_open_abort(db);
  }
  if((db->name = rydb_strdup(name)) == NULL) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory to open RyDB");
    return rydb_open_abort(db);
  }
  
  size_t sz = strlen(db->path);
  if(sz > 0 && db->path[sz - 1] == PATH_SLASH_CHAR) { // remove trailing slash
    *(char *)&db->path[sz - 1] = '\00';
  }
  
  if(!rydb_file_open(db, "lock", &db->lock)) {
    return rydb_open_abort(db);
  }
  if(!rydb_lock(db, RYDB_LOCK_CLIENT)) {
    return rydb_open_abort(db);
  }
  
  //add primary index if it's not already defined
  if(db->config.id_len > 0 && rydb_find_index_num(db, "primary") == -1) {
    if(!rydb_config_add_index_hashtable(db, "primary", 0, db->config.id_len, 1, NULL)) {
      return rydb_open_abort(db);
    }
  }
  
  new_db = !rydb_data_file_exists(db);
  
  if(!rydb_file_open(db, "data", &db->data)) {
    return rydb_open_abort(db);
  }
  if(!rydb_file_ensure_writable_address(db, &db->data, db->data.file.start, RYDB_DATA_START_OFFSET)) {
    return rydb_open_abort(db);
  }
  db->data.data.start = &db->data.file.start[RYDB_DATA_START_OFFSET];
  db->data.data.end = db->data.file.end;
  memcpy(db->data.file.start, RYDB_DATA_HEADER_STRING, strlen(RYDB_DATA_HEADER_STRING)); //mark it
  
  if(!rydb_file_open(db, "meta", &db->meta)) {
    return rydb_open_abort(db);
  }
  
  if(new_db) {
    if(db->config.row_len == 0) {//row length was never set. can't do anything
      rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Cannot create new unconfigured database: row length not set");
      return rydb_open_abort(db);
    }
  }
  else {
    if(db->config.row_len == 0) { //unconfigured db
      if(!rydb_meta_load(db, &db->meta)) {
        return rydb_open_abort(db);
      }
    }
    else {
      //need to compare intialized and loaded configs
      rydb_t *loaded_db = rydb_new();
      if(!loaded_db) {
        rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory to load RyDB");
        return rydb_open_abort(db);
      }
      
      if(!rydb_meta_load(loaded_db, &db->meta)) {
        rydb_set_error(db, loaded_db->error.code, "Failed to load database: %s", loaded_db->error.str);
        rydb_close(loaded_db);
        return rydb_open_abort(db);
      }
      
      //compare configs
      if(!rydb_config_match(db, loaded_db, "configured", "loaded")) {
        rydb_close(loaded_db);
        return rydb_open_abort(db);
      }
      if(db->config.hash_key.permanent && memcmp(db->config.hash_key.value, loaded_db->config.hash_key.value, sizeof(db->config.hash_key.value)) != 0) {
        //i don't think this could happen in operation, but just in case...
        rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Refusing to overwrite a permanent hash key with the loaded one");
        rydb_close(loaded_db);
        return rydb_open_abort(db);
      }
      
      //copy over hash_key stuff
      db->config.hash_key = loaded_db->config.hash_key;
        
      //ok, everything matches
      rydb_close(loaded_db);
    }
    
  }
  
  //create index file array
  if(db->config.index_count > 0) {
    sz = sizeof(*db->index) * db->config.index_count;
    if((db->index = rydb_mem.malloc(sz))==NULL) {
      rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for index files");
      return rydb_open_abort(db);
    }
    memset(db->index, '\00', sz);
    
    for(int i = 0; i < db->config.index_count; i++) {
      if(db->config.index[i].flags & RYDB_INDEX_UNIQUE) {
        db->unique_index_count++;
      }
      db->index[i].index.fd = -1;
      db->index[i].data.fd = -1;
      switch(db->config.index[i].type) {
        case RYDB_INDEX_INVALID:
        case RYDB_INDEX_BTREE:
          rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Tried opening unsupported index \"%s\" type", db->config.index[i].name);
          return rydb_open_abort(db);
        case RYDB_INDEX_HASHTABLE:
          if(!rydb_index_hashtable_open(db, i)) {
            return rydb_open_abort(db);
          }
          break;
      }
    }
    
    //we'll be wanting to check all unique indices during row changes, so they should be made easy to locate
    if(db->unique_index_count > 0) {
      uint8_t n = 0;
      db->unique_index = rydb_mem.malloc(sizeof(*db->unique_index) * (off_t )db->unique_index_count);
      if(!db->unique_index) {
        rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for unique indices");
        return rydb_open_abort(db);
      }
      for(int i = 0; i < db->config.index_count; i++) {
        if(db->config.index[i].flags & RYDB_INDEX_UNIQUE) {
          db->unique_index[n++]=i;
        }
      }
    }
  }
  
  db->stored_row_size = ry_align(db->config.row_len + db->config.link_pair_count * sizeof(rydb_rownum_t), 8); //let's make sure the data 
  if(new_db) {
    db->config.hash_key.quality = getrandombytes(db->config.hash_key.value, sizeof(db->config.hash_key.value));
    rydb_meta_save(db);
  }
  db->config.hash_key.permanent = 1;
  
  if(!rydb_data_scan_tail(db)) {
    return 0;
  }
  db->state = RYDB_STATE_OPEN;
  return 1;
}

static int rydb_file_delete(rydb_t *db, rydb_file_t *f) {
  if (f->path && remove(f->path) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to delete file %s", f->path);
    return 0;
  }
  return 1;
}

int rydb_delete(rydb_t *db) {
  if(!rydb_file_delete(db, &db->data)) return 0;
  if(!rydb_file_delete(db, &db->meta)) return 0;
  if(!rydb_file_delete(db, &db->lock)) return 0;
  if(db->index) {
    for(int i = 0; i < db->config.index_count; i++) {
      if(!rydb_file_delete(db, &db->index[i].index)) return 0;
      if(!rydb_file_delete(db, &db->index[i].data)) return 0;
    }
  }
  return 1;
}

int rydb_close(rydb_t *db) {
  rydb_close_nofree(db);
  if(db->name && db->path) {
    if(!rydb_unlock(db, RYDB_LOCK_READ | RYDB_LOCK_WRITE | RYDB_LOCK_CLIENT)) {
      return 0;
    }
  }
  rydb_free(db);
  return 1;
}

/* "inspired" by getrandombytes() from Redis
 * Get random bytes, attempts to get them straight from /dev/urandom.
 * If /dev/urandom is not available, a weaker seed is used to generate the 
 * random bytes using siphash.
 * 
 * returns 1 if bytes are good quality, 0 of they're shit
 */
    struct shitseed_s {
      clock_t clocked;
      struct timeval tv;
      pid_t pid;
    };
int getrandombytes(unsigned char *p, size_t len) {
  FILE *fp = NULL;
  int good = 0;
  if(!rydb_debug_disable_urandom) {
    fp = fopen("/dev/urandom","r");
  }
  if (fp && fread(p,len,1,fp) == 1) {
    good = 1;
  }
  else {
    //generate shitty seed rydb_row_nextfrom timeofday, pid, and other nonrandom crap
    struct shitseed_s shitseed;
    memset(&shitseed, '\00', (sizeof(shitseed))); //no this isn't like that Debian OpenSSL bug, but it's for much the same reason... to shut valgrind up.
    shitseed.clocked = clock();
    gettimeofday(&shitseed.tv,NULL);
    shitseed.pid = getpid();
    uint64_t rnd;
    char *seed = "this is at least 16 bytes long";
    rnd = siphash((uint8_t *)&shitseed, sizeof(shitseed), (uint8_t *)seed);
    for(int n = len; n > 0; n -= 8, p+=8) {
      memcpy(p, &rnd, n > 8 ? 8 : n);
      rnd = siphash((uint8_t *)&rnd, sizeof(rnd), (uint8_t *)seed);
    }
    good = 0;
  }
  if(fp) {
    fclose(fp);
  }
  return good;
}


rydb_stored_row_t *rydb_rownum_to_row(const rydb_t *db, const rydb_rownum_t rownum) {
  char *start = db->data.data.start;
  rydb_stored_row_t *row = rydb_row_next(start, db->stored_row_size, rownum - 1);
  if((char *)row < start || (char *)row > db->data.data.end) {
    return NULL;
  }
  return row;
}

rydb_rownum_t rydb_data_next_rownum(rydb_t *db) {
  return rydb_row_to_rownum(db, db->data_next_row);
}

rydb_rownum_t rydb_row_to_rownum(const rydb_t *db, const rydb_stored_row_t *row) {
  return (rydb_rownum_t )(1 + ((char *)row - db->data.data.start)/db->stored_row_size);
}

void rydb_data_update_last_nonempty_data_row(rydb_t *db, rydb_stored_row_t *row_just_emptied) {
  size_t              rowsz = db->stored_row_size;
  rydb_stored_row_t  *last = rydb_row_next(db->data_next_row, rowsz, -1);
  if(last != row_just_emptied) { 
    return; //end of data hasn't changed, the row that was just deleted wasn't at the tail-end of the data rows
  }
  // remove contiguous empty rows at the end of the data from the data range
  // this gives the DELETE command a worst-case performance of O(n)
  rydb_stored_row_t  *first = (void *)db->data.data.start;
  rydb_stored_row_t   *cur;
  for(cur = last; cur >= first; cur = rydb_row_next(cur, rowsz, -1)) {
    if(cur->type != RYDB_ROW_EMPTY) {
      break;
    }
  }
  db->data_next_row = rydb_row_next(cur, rowsz, 1);
}

int rydb_row_insert(rydb_t *db, const char *data, uint16_t len) {
  if(!rydb_ensure_open(db)) {
    return 0;
  }
  if(len == 0 || len > db->config.row_len) {
    len = db->config.row_len;
  }
  
  int txstarted;
  rydb_transaction_start_or_continue(db, &txstarted);
  
  rydb_row_t rows[2] = {
    {.type = RYDB_ROW_CMD_SET, .data=data, .len = len, .num = db->transaction.future_data_rownum++},
    {.type = RYDB_ROW_CMD_COMMIT, .num = 0}
  };
  if(!rydb_data_append_cmd_rows(db, rows, 1 + txstarted)) {
    if(txstarted) db->transaction.active = 0;
    return 0;
  }
  return rydb_transaction_finish_or_continue(db, txstarted);
}

int rydb_row_insert_str(rydb_t *db, const char *data) {
  return rydb_row_insert(db, data, strlen(data)+1);
}

int rydb_rownum_in_data_range(rydb_t *db, rydb_rownum_t rownum) {
  if(rownum < 1) {
    rydb_set_error(db, RYDB_ERROR_ROWNUM_OUT_OF_RANGE, "Rownum cannot be 0 (valid rownums start at 1)");
    return 0;
  }
  if(rownum >= rydb_data_next_rownum(db)) {
    rydb_set_error(db, RYDB_ERROR_ROWNUM_OUT_OF_RANGE, "Rownum exceeds total data rows");
    return 0;
  }
  return 1;
}

int rydb_row_update(rydb_t *db, const rydb_rownum_t rownum, const char *data, const uint16_t start, const uint16_t len) {
  if(!rydb_ensure_open(db)) {
    return 0;
  }
  if(start + len > db->config.row_len) {
    rydb_set_error(db, RYDB_ERROR_DATA_TOO_LARGE, "Data length to update exceeds row length");
    return 0;
  }
  if(!rydb_rownum_in_data_range(db, rownum)) {
    return 0;
  }
  
  int rc = 0;

  int txstarted;
  rydb_transaction_start_or_continue(db, &txstarted);
  
  uint16_t max_sz_for_1cmd_update = db->config.row_len - sizeof(rydb_row_cmd_header_t);
  rydb_row_t rows[3];
  
  if(len < max_sz_for_1cmd_update) {
    rows[0] = (rydb_row_t ){.type = RYDB_ROW_CMD_UPDATE, .num = rownum, .start = start, .len = len, .data = data};
    rows[1] = (rydb_row_t ){.type = RYDB_ROW_CMD_COMMIT, .num = 0};
    rc = rydb_data_append_cmd_rows(db, rows, 1 + txstarted);
  }
  else {
    rows[0] = (rydb_row_t ){.type = RYDB_ROW_CMD_UPDATE1, .num = rownum, .start = start, .len = len, .data = NULL};
    rows[1] = (rydb_row_t ){.type = RYDB_ROW_CMD_UPDATE2, .num = 0, .len = len, .data = data};
    rows[2] = (rydb_row_t ){.type = RYDB_ROW_CMD_COMMIT, .num = 0};
    rc = rydb_data_append_cmd_rows(db, rows, 2 + txstarted);
  }
  if(!rc) {
    if(txstarted) db->transaction.active = 0;
    return 0;
  }
  return rydb_transaction_finish_or_continue(db, txstarted);
}

int rydb_row_delete(rydb_t *db, rydb_rownum_t rownum) {
  if(!rydb_ensure_open(db)) {
    return 0;
  }
  if(!rydb_rownum_in_data_range(db, rownum)) {
    return 0;
  }
  
  int txstarted;
  rydb_transaction_start_or_continue(db, &txstarted);
  
  rydb_row_t rows[]={
    {.type = RYDB_ROW_CMD_DELETE, .num = rownum, .data=NULL},
    {.type = RYDB_ROW_CMD_COMMIT, .num = 0}
  };
  if(!rydb_data_append_cmd_rows(db, rows, 1 + txstarted)) {
    if(txstarted) db->transaction.active = 0;
    return 0;
  }
  return rydb_transaction_finish_or_continue(db, txstarted);
}

int rydb_row_swap(rydb_t *db, rydb_rownum_t rownum1, rydb_rownum_t rownum2) {
  if(!rydb_ensure_open(db)) {
    return 0;
  }
  if(!rydb_rownum_in_data_range(db, rownum1) || !rydb_rownum_in_data_range(db, rownum2)) {
    return 0;
  }
  if(rownum1 == rownum2) {
    //swap row with itself, do nothing.
    return 1;
  }
  
  int txstarted;
  rydb_transaction_start_or_continue(db, &txstarted);
  rydb_row_t rows[]={
    {.type = RYDB_ROW_CMD_SWAP1, .num = rownum1, .data = NULL},
    {.type = RYDB_ROW_CMD_SWAP2, .num = rownum2, .data = NULL},
    {.type = RYDB_ROW_CMD_COMMIT}
  };
  if(!rydb_data_append_cmd_rows(db, rows, 2 + txstarted)) {
    if(txstarted) db->transaction.active = 0;
    return 0;
  }
  return rydb_transaction_finish_or_continue(db, txstarted);
}

/*
int rydb_row_update(rydb_t *db, rydb_rownum_t rownum, char *data, uint16_t start, uint16_t len) {
  rydb_row_t update1 = {.type = RYDB_ROW_CMD_UPDATE1};
  rydb_row_t update2 = {.type = RYDB_ROW_CMD_UPDATE2};
  rydb_row_t commit = {.type = RYDB_ROW_CMD_COMMIT};
  rydb_row_t *rows[]={row, &commit};
  char update_data[sizeof(rydb_rownum_t) + sizeof(uint16_t)*2+1];
  char *cur = update_data;
  *(rydb_rownum_t *)cur = rownum;
  cur = &cur[sizeof(rydb_row_next)];
  *(uint16_t)cur = 
  
  
  if(!rydb_data_append_rows(db, rows, 2)) {
    return 0;
  }
}
*/

void rydb_print_stored_data(rydb_t *db) {
  const char *rowtype;
  char header[RYDB_DATA_START_OFFSET + 1];
  memcpy(header, db->data.file.start, RYDB_DATA_START_OFFSET);
  header[RYDB_DATA_START_OFFSET] = '\00';
  char datacpy[64];
  printf("\n>>%s\n", header);
  char rowtype_symbol[10];
  
  RYDB_EACH_ROW(db, cur) {
    if(cur->type == 0) {
      sprintf(rowtype_symbol, "\u2400");
    }
    else if(cur->type >= 32 && cur->type <= 126) {
      sprintf(rowtype_symbol, "%c", cur->type);
    }
    else {
      sprintf(rowtype_symbol, "\ufffd");
    }
    char *trail = "";
    rowtype = rydb_rowtype_str(cur->type);
    rowtype = &rowtype[strlen("RYDB_ROW_")];
    char *command = " ";
    
    if(memcmp(rowtype, "CMD_", 4) == 0) {
      command = "\u21b5";
      rowtype = &rowtype[strlen("CMD_")];
    }
    size_t len = db->stored_row_size - RYDB_ROW_DATA_OFFSET;
    if(len > 62) {
      len = 60;
      trail = "...";
    }
    memcpy(datacpy, cur->data, len);
    printf("[%3"PRIu32"]%s%7s%s <%3"PRIu32"> %s%s\n", rydb_row_to_rownum(db, cur), rowtype_symbol, rowtype, command, cur->target_rownum, datacpy, trail);
  }
}
