#include "rydb_internal.h"
#include "rydb_hashtable.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <signal.h>

#if defined _WIN32 || defined __CYGWIN__
#define PATH_SLASH_CHAR '\\'
#define PATH_SLASH "\\"
#else
#define PATH_SLASH_CHAR '/'
#define PATH_SLASH "/"
#endif


#define RYDB_PAGESIZE (sysconf(_SC_PAGE_SIZE))

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

#define RETURN_ERROR_PRINTF(err, func, ...) \
  if(err->errno_val != 0)            \
    return func(__VA_ARGS__ "ERROR [%d]: %s, errno [%d]: %s\n", err->code, err->str, err->errno_val, strerror(err->errno_val));\
  else                               \
    return func(__VA_ARGS__ "ERROR [%d]: %s\n", err->code, err->str)

int rydb_error_print(const rydb_t *db) {
  const rydb_error_t *err = &db->error;
  RETURN_ERROR_PRINTF(err, printf, "");
}

int rydb_error_fprint(const rydb_t *db, FILE *file) {
  const rydb_error_t *err = &db->error;
  RETURN_ERROR_PRINTF(err, fprintf, file, "");
}
int rydb_error_snprint(const rydb_t *db, char *buf, size_t buflen) {
  const rydb_error_t *err = &db->error;
  RETURN_ERROR_PRINTF(err, snprintf, buf, buflen, "");
}

rydb_error_t *rydb_error(const rydb_t *db) {
  if(db->error.code != RYDB_NO_ERROR) {
    return (rydb_error_t *)&db->error;
  }
  else {
    return NULL;
  }
}

void rydb_error_clear(rydb_t *db) {
  db->error.code = RYDB_NO_ERROR;
  db->error.errno_val = 0;
  db->error.str[0]='\00';
}

int rydb_set_error_handler(rydb_t *db, void (*fn)(rydb_t *, rydb_error_t *, void*), void *pd) {
  db->error_handler.function = fn;
  db->error_handler.privdata = pd;
  return 1;
}

void rydb_set_error(rydb_t *db, rydb_error_code_t code, const char *err_fmt, ...) {
  va_list ap;
  va_start(ap, err_fmt);
  vsnprintf(db->error.str, RYDB_ERROR_MAX_LEN-1, err_fmt, ap);
  db->error.code = code;
  db->error.errno_val = errno;
  va_end(ap);
  if(db->error_handler.function) {
    db->error_handler.function(db, &db->error, db->error_handler.privdata);
  }
}

rydb_t *rydb_new(void) {
  rydb_t *db = malloc(sizeof(*db));
  if(!db) {
    return NULL;
  }
  memset(db, '\00', sizeof(*db));
  db->data.fd = -1;
  db->meta.fd = -1;
  return db;
}

int rydb_config_row(rydb_t *db, unsigned row_len, unsigned id_len) {
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
  if(revision > RYDB_REVISION_MAX) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Revision number cannot exceed %"PRIu64, RYDB_REVISION_MAX);
    return 0;
  }
  return 1;
}

int rydb_config_add_row_link(rydb_t *db, const char *link_name, const char *reverse_link_name) {
  int            i;
  if(strlen(link_name) == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid row-link name of length 0.");
    return 0;
  }
  if(strlen(reverse_link_name) == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid reverse row-link name of length 0.");
    return 0;
  }
  if(!is_alphanumeric(link_name)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid row-link name, must be alphanumeric or underscores.");
    return 0;
  }
  if(!is_alphanumeric(reverse_link_name)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid reverse row-link name, must be alphanumeric or underscores.");
    return 0;
  }
  if(strcmp(link_name, reverse_link_name) == 0) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link and reverse row-link cannot be the same.");
    return 0;
  }
  
  for(i=0; i < db->config.link_pair_count*2; i++) {
    if(strcmp(link_name, db->config.link[i].next) == 0) {
      rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link with name \"%s\" already exists.", link_name);
      return 0;
    }
    if(strcmp(reverse_link_name, db->config.link[i].next) == 0) {
      rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Row-link with name \"%s\" already exists.", reverse_link_name);
      return 0;
    }
  }
  
  if(db->config.link_pair_count == 0) {
    db->config.link = malloc(sizeof(*db->config.link)*2);
  }
  else {
    db->config.link = realloc(db->config.link, sizeof(*db->config.link)*(db->config.link_pair_count + 1)*2);
  }
  if(db->config.link == NULL) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for row-link");
    return 0;
  }
  rydb_config_row_link_t *link, *link_inverse;
  off_t offset = db->config.link_pair_count * 2;
  link = &db->config.link[offset];
  link_inverse = &db->config.link[offset+1];
  
  link->inverse = 0;
  link->next = strdup(link_name);
  if(!link->next) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for row-link");
    return 0;
  }
  link->prev = strdup(reverse_link_name);
  if(!link->prev) {
    free((char *)link->next);
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Failed to allocate memory for row-link");
    return 0;
  }
  
  link_inverse->inverse = 1;
  link_inverse->next = link->prev;
  link_inverse->prev = link->next;
  db->config.link_pair_count ++;
  
  return 1;
}

static int index_config_compare(const void *v1, const void *v2) {
  const rydb_config_index_t *idx1 = v1;
  const rydb_config_index_t *idx2 = v2;
  return strcmp(idx1->name, idx2->name);
}

static int rydb_config_add_index(rydb_t *db, rydb_config_index_t *idx) {
  if(strlen(idx->name) > RYDB_INDEX_NAME_MAX_LEN) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index name \"%s\" too long, must be at most %i characters", idx->name, RYDB_INDEX_NAME_MAX_LEN);
    return 0;
  }
  if(!is_alphanumeric(idx->name)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index name \"%s\" invalid: must consist of only ASCII alphanumeric characters and underscores", idx->name);
    return 0;
  }
  if(!rydb_index_type_valid(idx->type)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" type for is invalid", idx->name);
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
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Index \"%s\" already exists");
    return 0;
  }
  
  //allocation
  if(db->config.index_count == 0) {
    db->config.index = malloc(sizeof(*db->config.index));
  }
  else {
    db->config.index = realloc(db->config.index, sizeof(*db->config.index) * (db->config.index_count + 1));
  }
  if(db->config.index == NULL) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for index \"%s\"", idx->name);
    return 0;
  }
  rydb_config_index_t *new_idx = &db->config.index[db->config.index_count];
  
  *new_idx = *idx;
  if((new_idx->name = strdup(idx->name)) == NULL) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for index \"%s\" name", idx->name);
    return 0;
  }
  
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
  if(!cf){
    return -1;
  }
  else {
    return cf - cf_start;
  }
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
                  strlen(db->path)> 0 ? PATH_SLASH : "", 
                  db->name, 
                  strlen(db->name)> 0 ? "." : "", 
                  what);
}
static void rydb_subfree(const void *ptr) {
  if(ptr) {
    free((void *)ptr);
  }
}

static void rydb_free(rydb_t *db) {
  unsigned i;
  rydb_subfree(db->path);
  rydb_subfree(db->name);
  for(i=0; i<db->config.index_count; i++) {
    if(db->config.index) {
      rydb_subfree(db->config.index[i].name);
    }
  }
  rydb_subfree(db->config.index);
  rydb_subfree(db->index);
  
  if(db->config.link) {
    for(i=0; i< db->config.link_pair_count*2; i++) {
      rydb_subfree(db->config.link[i].next);
    }
    rydb_subfree(db->config.link);
  }
  
  free(db);
}

static int rydb_lock(rydb_t *db) {
  char buf[1024];
  rydb_filename(db, "lock", buf, 1024);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;
  int fd = open(buf, O_CREAT | O_EXCL, mode);
  if(fd == -1) {
    if(errno == EEXIST) {
      rydb_set_error(db, RYDB_ERROR_LOCK_FAILED, "Database is already locked");
    }
    else {
      rydb_set_error(db, RYDB_ERROR_LOCK_FAILED, "Can't lock database");
    }
    return 0;
  }
  else {
    //lock file created, i don't think we need to keep its fd open
    close(fd);
    return 1;
  }
}

static int rydb_unlock(rydb_t *db) {
    char buf[1024];
  rydb_filename(db, "lock", buf, 1024);
  if(access(buf, F_OK) == -1){ //no lock present, nothing to unlock
    return 1;
  }
  if(remove(buf) == 0) {
    return 1;
  }
  else {
    return 0;
  }
}

/*
static int rydb_file_ensure_size(rydb_t *db, rydb_file_t *f, size_t desired_min_sz) {
  size_t current_sz = f->file.end - f->file.start;
  if(current_sz < desired_min_sz) {
    if(lseek(f->fd, desired_min_sz - current_sz, SEEK_END) == -1) {
      rydb_set_error(db, RYDB_ERROR_FILE_SIZE, "Failed to seek to end of file");
      return 0;
    }
    if(write(f->fd, "\00", 1) == -1) {
      rydb_set_error(db, RYDB_ERROR_FILE_SIZE, "Failed to grow file");
      return 0;
    }
  }
  return 1;
}
*/

static int rydb_file_getsize(rydb_t *db, int fd, off_t *sz) {
  struct stat st;
  if(fstat(fd, &st)) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed to get filesize");
    return 0;
  }
  *sz = st.st_size;
  return 1;
}

static int rydb_file_close(rydb_t *db, rydb_file_t *f) {
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
    free((char *)f->path);
    f->path = NULL;
  }
  return ok;
}

int rydb_file_open(rydb_t *db, const char *what, rydb_file_t *f) {
  off_t sz;
  char path[2048];
  rydb_filename(db, what, path, 2048);
  
  if((f->path = strdup(path)) == NULL) { //useful for debugging
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
  }
  
  sz = RYDB_PAGESIZE*10;
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
  else if(strcmp(str, "B-tree") == 0) {
    return RYDB_INDEX_BTREE;
  }
  else {
    return RYDB_INDEX_INVALID;
  }
}

static int rydb_meta_save(rydb_t *db) {
  FILE     *fp = db->meta.fp;
  int       rc, i;
  int       total_written = 0;
  rydb_config_index_t *idxcf;
  
  if(fseek(fp, 0, SEEK_SET) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Failed seeking to start of meta file %s", db->meta.path);
    return 0;
  }
  
  const char *fmt = 
    "--- #rydb\n"
    "format_revision: %i\n"
    "database_revision: %"PRIu32"\n"
    "endianness: %s\n"
    "row_len: %"PRIu16"\n"
    "id_len: %"PRIu16"\n"
    "index_count: %"PRIu16"\n"
    "%s";
  rc = fprintf(fp, fmt, RYDB_FORMAT_VERSION, db->config.revision, is_little_endian() ? "little" : "big", db->config.row_len, db->config.id_len, db->config.index_count, db->config.index_count > 0 ? "index:" : "");
  if(rc <= 0) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to meta file %s", db->meta.path);
    return 0;
  }
  total_written += rc;
  
  const char *index_fmt = "\n"
    "  - name: %s\n"
    "    type: %s\n"
    "    start: %"PRIu16"\n"
    "    len: %"PRIu16"\n"
    "    unique: %"PRIu16"\n";
  
  for(i=0; i<db->config.index_count; i++) {
    idxcf = &db->config.index[i];
    rc = fprintf(fp, index_fmt, idxcf->name, rydb_index_type_str(idxcf->type), idxcf->start, idxcf->len, (uint16_t )(idxcf->flags & RYDB_INDEX_UNIQUE));
    if(rc <= 0){
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

  for(i=0; i < db->config.link_pair_count*2; i++) {
    if(!db->config.link[i].inverse) {
      rc = fprintf(fp, "  - [ %s , %s ]\n", db->config.link[i].next, db->config.link[i].prev);
      if(rc <= 0) {
        rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "Failed writing header to meta file %s", db->meta.path);
        return 0;
      }
      total_written += rc;
    }
  }
  
  return total_written;
}



#define QUOTE(str) #str
#define EXPAND_AND_QUOTE(str) QUOTE(str)
#define RYDB_INDEX_NAME_MAX_LEN_STR EXPAND_AND_QUOTE(RYDB_INDEX_NAME_MAX_LEN)

static int rydb_meta_load(rydb_t *db, rydb_file_t *ryf) {
  unsigned  i;
  FILE     *fp = ryf->fp;
  char      endianness_buf[16];
  int       little_endian;
  uint16_t  rydb_format_version, db_revision, row_len, id_len, index_count;
  if(fseek(fp, 0, SEEK_SET) == -1) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Failed seeking to start of data file");
    return 0;
  }
  
  const char *fmt = 
    "--- #rydb\n"
    "format_revision: %hu\n"
    "database_revision: %hu\n"
    "endianness: %15s\n"
    "row_len: %hu\n"
    "id_len: %hu\n"
    "index_count: %hu\n";
  int rc = fscanf(fp, fmt, &rydb_format_version, &db_revision, endianness_buf, &row_len, &id_len, &index_count);
  if(rc < 6){
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Not a RyDB file or is corrupted");
    return 0;
  }
  if(rydb_format_version != RYDB_FORMAT_VERSION) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Format version mismatch, expected %i, loaded %"PRIu16, RYDB_FORMAT_VERSION, rydb_format_version);
    return 0;
  }
  if(index_count > RYDB_INDICES_MAX) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "File invalid, too many indices defined");
    return 0;
  }
  
  if(!rydb_config_row(db, row_len, id_len))
    return 0;
  
  if(!rydb_config_revision(db, db_revision))
    return 0;
  
  if(strcmp(endianness_buf, "big") == 0)
    little_endian = 0;
  else if(strcmp(endianness_buf, "little") == 0)
    little_endian = 1;
  else {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "File invalid, unexpected endianness %s", endianness_buf);
    return 0;
  }
  
  if(is_little_endian() != little_endian) {
    //TODO: convert data to host endianness
    rydb_set_error(db, RYDB_ERROR_WRONG_ENDIANNESS, "File has wrong endianness");
    return 0;
  }
  
  const char *index_fmt = "\n"
    "  - name: %" RYDB_INDEX_NAME_MAX_LEN_STR "s\n"
    "    type: %32s\n"
    "    start: %hu\n"
    "    len: %hu\n"
    "    unique: %hu\n";
  
  char                      index_name_buf[RYDB_INDEX_NAME_MAX_LEN];
  char                      index_type_buf[32];
  uint16_t                  index_unique;
  rydb_config_index_t       idx_cf;
  
  if(index_count > 0) {
    if(fscanf(fp, "index:") < 0) {
      rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "index specification is corrupted or invalid");
      return 0;
    }
    for(i=0; i<index_count; i++) {
      rc = fscanf(fp, index_fmt, index_name_buf, index_type_buf, &idx_cf.start, &idx_cf.len, &index_unique);
      if(rc < 5){
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
  uint16_t                  linkpairs_count;
  char                      link_next_buf[RYDB_INDEX_NAME_MAX_LEN], link_prev_buf[RYDB_INDEX_NAME_MAX_LEN];
  rc = fscanf(fp, "link_pair_count: %hu\n", &linkpairs_count);
  if(rc < 1 || linkpairs_count*2 > RYDB_ROW_LINKS_MAX) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "link specification is corrupted or invalid");
    return 0;
  }
  if(linkpairs_count > 0) {
    if(fscanf(fp, "link_pair:\n") < 0) {
      rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "link specification is corrupted or invalid");
      return 0;
    }
    for(i=0; i < linkpairs_count; i++) {
      rc = fscanf(fp, "  - [ %" RYDB_INDEX_NAME_MAX_LEN_STR "s , %" RYDB_INDEX_NAME_MAX_LEN_STR "s ]\n", link_next_buf, link_prev_buf);
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
  int i;
  //see if the loaded config and the one passed in are the same
  if(db->config.revision != db2->config.revision) {
    rydb_set_error(db, RYDB_ERROR_REVISION_MISMATCH, "Wrong revision number: %s %"PRIu32", %s %"PRIu32, db_lbl, db->config.revision, db2_lbl, db2->config.revision);
    return 0;
  }
  if(db->config.row_len != db2->config.row_len) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong row length: %s %"PRIu16", %s %"PRIu32, db_lbl, db->config.row_len, db2_lbl, db2->config.row_len);
    return 0;
  }
  if(db->config.id_len != db2->config.id_len) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong id length: %s %"PRIu16", %s %"PRIu16, db_lbl, db->config.id_len, db2_lbl, db2->config.id_len);
    return 0;
  }
  if(db->config.index_count != db2->config.index_count) {
    rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index count: %s %"PRIu16", %s %"PRIu16, db_lbl, db->config.index_count, db2_lbl, db2->config.index_count);
    return 0;
  }
      
  //compare indices
  rydb_config_index_t *expected_index_cf;
  for(i=0; i<db2->config.index_count; i++) {
    expected_index_cf = &db->config.index[i];
    if(strcmp(expected_index_cf->name, db2->config.index[i].name) != 0) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i name: expected %s, loaded %s", i, expected_index_cf->name, db2->config.index[i].name);
      return 0;
    }
    if(expected_index_cf->type != db2->config.index[i].type) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i type: expected %s, loaded %s", i, rydb_index_type_str(expected_index_cf->type), rydb_index_type_str(db2->config.index[i].type));
      return 0;
    }
    if(expected_index_cf->start != db2->config.index[i].start) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i start: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->start, db2->config.index[i].start);
      return 0;
    }
    if(expected_index_cf->len != db2->config.index[i].len) {
      rydb_set_error(db, RYDB_ERROR_CONFIG_MISMATCH, "Wrong index %i length: expected %"PRIu16", loaded %"PRIu16, i, expected_index_cf->len, db2->config.index[i].len);
      return 0;
    }
  }
  return 1;
}

static int rydb_data_file_exists(const rydb_t *db) {
  char path[1024];
  rydb_filename(db, "data", path, 1024);
  return access(path, F_OK) != -1;
}

static void rydb_close_nofree(rydb_t *db) {
  int i;
  rydb_file_close(db, &db->data);
  rydb_file_close(db, &db->meta);
  if(db->index) {
    for(i = 0; i<db->config.index_count; i++) {
      rydb_file_close(db, &db->index[i].index);
      rydb_file_close(db, &db->index[i].data);
    }
  }
}

static int rydb_open_abort(rydb_t *db) {
  rydb_close_nofree(db);
  return 0;
}



int rydb_open(rydb_t *db, const char *path, const char *name) {
  int           new_db = 0, i;
  char         *dup_path = strdup(path);
  
  size_t sz = strlen(dup_path);
  if(sz > 0 && dup_path[sz-1]==PATH_SLASH_CHAR) { // remove trailing slash
    dup_path[sz-1] = '\00';
  }
  
  db->name = strdup(name);
  db->path = dup_path;
  
  if(!rydb_lock(db)) {
    return rydb_open_abort(db);
  }
  
  if(!db->name || !db->path) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory to open RyDB");
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
  if(!rydb_file_open(db, "meta", &db->meta)) {
    return rydb_open_abort(db);
  }
  
  if(new_db) {
    if(db->config.row_len == 0) {//row length was never set. can't do anything
      rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Cannot create new unconfigured database: row length not set");
      return rydb_open_abort(db);
    }
    rydb_meta_save(db);
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
      //ok, everything matches
      rydb_close(loaded_db);
    }
  }
  
  //create index file array
  
  sz = sizeof(*db->index) * db->config.index_count;
  db->index = malloc(sz);
  if(!db->index) {
    rydb_set_error(db, RYDB_ERROR_NOMEMORY, "Unable to allocate memory for index files");
    return rydb_open_abort(db);
  }
  memset(db->index, '\00', sz);
  
  for(i=0; i<db->config.index_count; i++) {
    db->index[i].index.fd = -1;
    db->index[i].data.fd = -1;
    switch(db->config.index[i].type) {
      case RYDB_INDEX_INVALID:
      case RYDB_INDEX_BTREE:
        rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Tried opening unsupported index \"%s\" type", db->config.index[i].name);
        return rydb_open_abort(db);
      case RYDB_INDEX_HASHTABLE:
        printf("opening hashtable %s %i\n", db->config.index[i].name, i);
        if(!rydb_index_hashtable_open(db, i)) {
          return rydb_open_abort(db);
        }
        break;
    }
    
  }
  
  if(new_db) {
    rydb_meta_save(db);
  }
  
  return 1;
}

int rydb_close(rydb_t *db) {
  rydb_close_nofree(db);
  if(db->name && db->path) {
    if(!rydb_unlock(db)) {
      return 0;
    }
  }
  rydb_free(db);
  return 1;
}


