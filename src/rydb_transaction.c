#include "rydb_internal.h"
#include <string.h>
#include <assert.h>

static bool rydb_cmd_rangecheck(rydb_t *db, const char *cmdname, rydb_stored_row_t *cmd, rydb_stored_row_t *dst) {
  if(!dst || !rydb_stored_row_in_range(db, dst)) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command %s [%i] failed: rownum out of range", cmdname, cmd->target_rownum);
    cmd->type = RYDB_ROW_EMPTY;
    return false;
  }
  if(dst > cmd) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command %s [%i] failed: rownum is beyond command rownum", cmdname, cmd->target_rownum);
    cmd->type = RYDB_ROW_EMPTY;
    return false;
  }
  return true;
}

bool rydb_data_append_cmd_rows(rydb_t *db, rydb_row_t *rows, const off_t count) {
  uint_fast16_t        rowsize = db->stored_row_size;
  if(!rydb_file_ensure_size(db, &db->data, (db->cmd_next_rownum + count) * rowsize, NULL)) {
    return false;
  }
  rydb_stored_row_t   *newrows_start = rydb_rownum_to_row(db, db->cmd_next_rownum);
  rydb_stored_row_t   *newrows_end = rydb_row_next(newrows_start, rowsize, count);
  rydb_stored_row_t *cur = newrows_start;
  for(int i=0; i<count; i++) {
    //copy the data
    rydb_row_t    *row = &rows[i];
    uint_fast16_t  rowlen = row->len;
    rydb_row_cmd_header_t *header;
    switch(rows[i].type) {
      case RYDB_ROW_CMD_SET:
        memcpy(cur->data, row->data, rowlen > 0 ? rowlen : db->config.row_len);
        break;
      case RYDB_ROW_CMD_UPDATE:
        header = (void *)cur->data;
        header->len = rowlen;
        header->start = row->start;
        memcpy((char *)&header[1], row->data, rowlen);
        break;
      case RYDB_ROW_CMD_UPDATE1:
        header = (void *)cur->data;
        header->len = rowlen;
        header->start = row->start;
        break;
      case RYDB_ROW_CMD_UPDATE2:
        memcpy(cur->data, row->data, rowlen);
        break;
      case RYDB_ROW_CMD_DELETE:
      case RYDB_ROW_CMD_SWAP1:
      case RYDB_ROW_CMD_SWAP2:
      case RYDB_ROW_CMD_COMMIT:
        //copy no data
        break;
      case RYDB_ROW_EMPTY:
      case RYDB_ROW_DATA:
        rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Tried to append row %s to transaction log", rydb_rowtype_str(row->type));
        return false;
    }
    cur->target_rownum = rows[i].num;
    cur->type = rows[i].type;
    cur = rydb_row_next(cur, rowsize, 1);
  }
  db->cmd_next_rownum = rydb_row_to_rownum(db, newrows_end);
  return true;
}

static inline bool rydb_cmd_set(rydb_t *db, rydb_stored_row_t *cmd) {
  rydb_stored_row_t   *dst = rydb_rownum_to_row(db, cmd->target_rownum);
  if(!rydb_cmd_rangecheck(db, "SET", cmd, dst)) {
    return false;
  }
  if(cmd != dst && dst->type == RYDB_ROW_DATA) {
    rydb_indices_remove_row(db, dst);
  }
  
  if(!rydb_indices_add_row(db, dst)) {
    return false;
  }
  
  if(cmd == dst) {
    //set this very rownum
    cmd->type = RYDB_ROW_DATA;
    cmd->target_rownum = 0; //clear target
  }
  else {
    memcpy(dst->data, cmd->data, db->config.row_len);
    dst->type = RYDB_ROW_DATA;
    cmd->type = RYDB_ROW_EMPTY;
  }
  rydb_rownum_t dst_rownum = rydb_row_to_rownum(db, dst);
  if(dst_rownum >= db->data_next_rownum) {
    db->data_next_rownum = dst_rownum + 1;
  }
  return true;
}

static inline bool rydb_cmd_update(rydb_t *db, rydb_stored_row_t *cmd) {
  rydb_row_cmd_header_t    *header = (rydb_row_cmd_header_t *)(void *)cmd->data;
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd->target_rownum);
  if(!rydb_cmd_rangecheck(db, "UPDATE", cmd, dst)) {
    return false;
  }
  char *update_data = (char *)&header[1];
  rydb_indices_update_row(db, dst, 0, header->start, header->len);
  memcpy(&dst->data[header->start], update_data, header->len);
  rydb_indices_update_row(db, dst, 1, header->start, header->len);
  cmd->type = RYDB_ROW_EMPTY;
  return true;
}

static inline bool rydb_cmd_update1(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd2 || cmd2->type != RYDB_ROW_CMD_UPDATE2) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command UPDATE2 [%"RYPRIrn"] failed: second command row has the wrong type or is missing", cmd2 ? rydb_row_to_rownum(db, cmd2) : 0);
    cmd1->type = RYDB_ROW_EMPTY;
    if(cmd2) {
      cmd2->type = RYDB_ROW_EMPTY;
    }
    return false;
  }
  //do nothing if everything is ok
  return true;
}
static inline bool rydb_cmd_update2(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd1) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command UPDATE2 [%"RYPRIrn"] failed: one of the UPDATE2 rows is missing", rydb_row_to_rownum(db, cmd2));
    cmd2->type = RYDB_ROW_EMPTY;
    return false;
  }
  rydb_row_cmd_header_t    *header = (rydb_row_cmd_header_t *)(void *)cmd1->data;
  if(cmd1->type != RYDB_ROW_CMD_UPDATE1 || cmd2->type != RYDB_ROW_CMD_UPDATE2) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command UPDATE2 [%"RYPRIrn"] failed: one of the UPDATE rows has the wrong type", rydb_row_to_rownum(db, cmd2));
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return false;
  }
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd1->target_rownum);
  if(!rydb_cmd_rangecheck(db, "UPDATE2", cmd1, dst)) {
    return false;
  }
  rydb_indices_update_row(db, dst, 0, header->start, header->len);
  memcpy(&dst->data[header->start], cmd2->data, header->len);
  cmd2->type = RYDB_ROW_EMPTY;
  cmd1->type = RYDB_ROW_EMPTY;
  rydb_indices_update_row(db, dst, 1, header->start, header->len);
  return true;
}
static inline bool rydb_cmd_delete(rydb_t *db, rydb_stored_row_t *cmd) {
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd->target_rownum);
  if(!rydb_cmd_rangecheck(db, "DELETE", cmd, dst)) {
    return false;
  }
  rydb_indices_remove_row(db, dst);
  dst->type = RYDB_ROW_EMPTY;
  cmd->type = RYDB_ROW_EMPTY;
  // remove contiguous empty rows at the end of the data from the data range
  // this gives the DELETE command a worst-case performance of O(n)
  // (but only when deleting the last row)
  rydb_data_update_last_nonempty_data_row(db, dst);
  return true;
}

static inline bool rydb_cmd_swap1(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd2) {//that's weird...
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: second command row is missing");
    cmd1->type = RYDB_ROW_EMPTY;
    return false;
  }
  rydb_stored_row_t  *src, *dst;
  switch(cmd2->type) {
    case RYDB_ROW_CMD_SWAP2:
      // don't need to be doing anything.
      // SWAP2 needs to run first to change itself to a SET or DELETE
      // then it will run SWAP1
      return true;
    case RYDB_ROW_CMD_SET:
    case RYDB_ROW_CMD_DELETE:
      //run the swap now, the temp src copy has already been written
      src = rydb_rownum_to_row(db, cmd1->target_rownum);
      dst = rydb_rownum_to_row(db, cmd2->target_rownum);
      if(!rydb_cmd_rangecheck(db, "SWAP", cmd1, src) || !rydb_cmd_rangecheck(db, "SWAP", cmd1, dst)) {
        return false;
      }
      memcpy(src, dst, db->stored_row_size);
      if(src->type == RYDB_ROW_EMPTY) {
        // remove contiguous empty rows at the end of the data from the data range
        // this gives the SWAP command a worst-case performance of O(n)
        // (but only when swapping with the last row)
        rydb_data_update_last_nonempty_data_row(db, src);
      }
      //the followup SET command will write src to dst;
      cmd1->type = RYDB_ROW_EMPTY;
      return true;
    default:
      rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed:  second command row has the wrong type");
      cmd1->type = RYDB_ROW_EMPTY;
      return false;
  }
}

static inline bool rydb_cmd_swap2(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd1) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP2 failed: SWAP1 is missing");
    return false;
  }
  rydb_stored_row_t  *src = rydb_rownum_to_row(db, cmd1->target_rownum);
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd2->target_rownum);
  if(!rydb_cmd_rangecheck(db, "SWAP2", cmd1, src) || !rydb_cmd_rangecheck(db, "SWAP2", cmd1, dst)) {
    return false;
  }
  
  //TODO: figure out efficient implementation for reindexing swaps
  
  size_t              rowsz = db->stored_row_size;
  
  if(cmd2->type != RYDB_ROW_CMD_SWAP2 || cmd1->type != RYDB_ROW_CMD_SWAP1) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: one of the command rows is the wrong type");
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return false;
  }
  
  rydb_row_type_t srctype = src->type, dsttype = dst->type;
  if(dsttype != RYDB_ROW_EMPTY && dsttype != RYDB_ROW_DATA) {
        rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: dst row is the wrong type");
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return false;
  }
  
  rydb_row_type_t cmd2type;
  switch(srctype) {
    case RYDB_ROW_DATA:
      memcpy(cmd2->data, src->data, rowsz - offsetof(rydb_stored_row_t, data)); //copy just the data
      cmd2->type = cmd2type = RYDB_ROW_CMD_SET; //second half of the swap is not a SET command.
      // it can be idempotently re-run in case db writes fail before the swap is completed
      break;
    case RYDB_ROW_EMPTY:
      cmd2->type = cmd2type = RYDB_ROW_CMD_DELETE;
      break;
    default:
      rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: src row is the wrong type");
      cmd1->type = RYDB_ROW_EMPTY;
      cmd2->type = RYDB_ROW_EMPTY;
      return false;
  }
  if(!rydb_cmd_swap1(db, cmd1, cmd2)) {
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return false;
  }
  bool ret = false;
  if(cmd2type == RYDB_ROW_CMD_SET) {
    ret = rydb_cmd_set(db, cmd2);
  }
  else if(cmd2type == RYDB_ROW_CMD_DELETE) {
    ret = rydb_cmd_delete(db, cmd2);
  }
  if(!ret) {
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return false;
  }
  return true;
}

bool rydb_transaction_run(rydb_t *db, rydb_stored_row_t *last_row_to_run) {
  rydb_stored_row_t *prev = NULL, *next;
  rydb_stored_row_t *lastcmd;
  if(last_row_to_run) {
    lastcmd = last_row_to_run;
  }
  else {
    lastcmd = rydb_rownum_to_row(db, db->cmd_next_rownum - 1);
  }
  if(lastcmd < rydb_rownum_to_row(db, db->data_next_rownum) || 
    (rydb_debug_refuse_to_run_transaction_without_commit && lastcmd->type != RYDB_ROW_CMD_COMMIT)) {
    // no CMD_COMMIT at the end -- bail
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_INCOMPLETE, "Refused to run a transaction that doesn't end with a COMMIT");
    return false;
  }
  bool ret = true;
  rydb_stored_row_t *commit_row = NULL;
  RYDB_EACH_CMD_ROW(db, cur) {
    if(last_row_to_run && cur > last_row_to_run) {
      break;
    }
    else if(ret) {
      switch((rydb_row_type_t )cur->type) {
        case RYDB_ROW_EMPTY:
        case RYDB_ROW_DATA:
          //do nothing, these are weird.
          break;
        case RYDB_ROW_CMD_SET:
          ret = rydb_cmd_set(db, cur);
          break;
        case RYDB_ROW_CMD_UPDATE:
          ret = rydb_cmd_update(db, cur);
          break;
        case RYDB_ROW_CMD_UPDATE1:
          next = rydb_row_next(cur, db->stored_row_size, 1);
          if(next >= rydb_rownum_to_row(db, db->cmd_next_rownum)) next = NULL;
          ret = rydb_cmd_update1(db, cur, next);
          break;
        case RYDB_ROW_CMD_UPDATE2:
          ret = rydb_cmd_update2(db, prev, cur);
          break;
        case RYDB_ROW_CMD_DELETE:
          ret = rydb_cmd_delete(db, cur);
          break;
        case RYDB_ROW_CMD_SWAP1:
          next = rydb_row_next(cur, db->stored_row_size, 1);
          if(next >= rydb_rownum_to_row(db, db->cmd_next_rownum)) next = NULL;
          ret = rydb_cmd_swap1(db, cur, next);
          break;
        case RYDB_ROW_CMD_SWAP2:
          ret = rydb_cmd_swap2(db, prev, cur);
          break;
        case RYDB_ROW_CMD_COMMIT:
          //clear it
          commit_row = cur;
          cur->type = RYDB_ROW_EMPTY;
          break;
      }
      prev = cur;
    }
    else if(cur->type != RYDB_ROW_DATA) {
      //the transaction failed -- clear it from the log
      cur->type = RYDB_ROW_EMPTY;
    }
  }
  if(!ret) {
    return false;
  }
  if(commit_row != prev) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Transaction was committed without ending on a COMMIT command");
    return false;
  }
  return ret;
}

static int uniqueness_check_comparator(const RBNode *a, const RBNode *b, void *arg) {
  return memcmp((const char *)(&a[1]), (const char *)(&b[1]), (size_t )(uintptr_t )arg);
}
static void uniqueness_check_combiner(UNUSED(RBNode *existing), UNUSED(const RBNode *newdata), UNUSED(void *arg)) { }
static RBNode *uniqueness_check_allocfunc(void *arg) {
  return rydb_mem.malloc(sizeof(RBNode) + (uintptr_t )arg);
}
void uniqueness_check_freefunc(RBNode *x, UNUSED(void *arg)) {
  free(x);
}

bool rydb_transaction_data_init(rydb_t *db) {
  db->transaction.active = 0;
  db->transaction.command_count = 0;
  
  if(db->unique_index_count > 0) {
    if((db->transaction.unique_index_constraints.added = rydb_mem.malloc(sizeof(RBTree) * db->unique_index_count)) == NULL) {
      return false;
    }
    if((db->transaction.unique_index_constraints.removed = rydb_mem.malloc(sizeof(RBTree) * db->unique_index_count)) == NULL) {
      free(db->transaction.unique_index_constraints.added);
      return false;
    }
    
    for(int i=0; i<db->unique_index_count; i++) {
      rydb_index_t *idx = db->unique_index[i];
      rb_create(&db->transaction.unique_index_constraints.added[i], sizeof(RBNode) + idx->config->len,
                uniqueness_check_comparator,
                uniqueness_check_combiner,
                uniqueness_check_allocfunc,
                uniqueness_check_freefunc,
                (void *)(uintptr_t)idx->config->len
              );
      rb_create(&db->transaction.unique_index_constraints.removed[i], sizeof(RBNode) + idx->config->len,
                uniqueness_check_comparator,
                uniqueness_check_combiner,
                uniqueness_check_allocfunc,
                uniqueness_check_freefunc,
                (void *)(uintptr_t )idx->config->len
              );
    }
  }
  return true;
}

void rydb_transaction_data_free(rydb_t *db) {
  rydb_transaction_data_reset(db);
  if(db->unique_index_count > 0) {
    rydb_mem.free(db->transaction.unique_index_constraints.added);
    rydb_mem.free(db->transaction.unique_index_constraints.removed);
  }
}

void rydb_transaction_data_reset(rydb_t *db) {
  db->transaction.active = 0;
  db->transaction.command_count = 0;
  if(!db->transaction.oneshot) {
    for(int i=0; i<db->unique_index_count; i++) {
      rb_free(&db->transaction.unique_index_constraints.added[i]);
      rb_free(&db->transaction.unique_index_constraints.removed[i]);
    }
  }
  db->transaction.oneshot = 0;
}

uint_fast8_t rydb_transaction_check_unique(rydb_t *db, const char *val, off_t i) {
  if(rb_find(&db->transaction.unique_index_constraints.removed[i], (const RBNode *)(void *)(val - sizeof(RBNode)))) {
    return 1;
  }
  if(rb_find(&db->transaction.unique_index_constraints.added[i], (const RBNode *)(void *)(val - sizeof(RBNode)))) {
    return 0;
  }
  return -1; //unknown
}

static bool rydb_transaction_unique_change(rydb_t *db, const char *val, bool add, off_t i) {
  RBNode *node, *searchnode = (RBNode *)(void *)(val - sizeof(RBNode));
  RBTree *remove_from, *add_to;
  if(add) {
    remove_from = &db->transaction.unique_index_constraints.removed[i];
    add_to = &db->transaction.unique_index_constraints.added[i];
  }
  else {
    remove_from = &db->transaction.unique_index_constraints.added[i];
    add_to = &db->transaction.unique_index_constraints.removed[i];
  }
  if((node = rb_find(remove_from, searchnode)) != NULL) {
    rb_delete(remove_from, node);
  }
  bool added;
  rb_insert(add_to, searchnode, &added);
  return true;
}

bool rydb_transaction_unique_add(rydb_t *db, const char *val, off_t i) {
  return rydb_transaction_unique_change(db, val, true, i);
}
bool rydb_transaction_unique_remove(rydb_t *db, const char *val, off_t i) {
  return rydb_transaction_unique_change(db, val, false, i);
}

bool rydb_transaction_finish_or_continue(rydb_t *db, int finish) {
  if(finish && db->transaction.active) {
    bool ret = rydb_transaction_run(db, NULL);
    //succeed or fail -- the transaction should be cleared
    rydb_transaction_data_reset(db);
    db->cmd_next_rownum = db->data_next_rownum;
    return ret;
  }
  return true;
}

bool rydb_transaction_finish(rydb_t *db) {
  if(!db->transaction.active) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_INACTIVE, "No active transaction to finish");
    return false;
  }
  rydb_row_t commit_row = {.type = RYDB_ROW_CMD_COMMIT, .num = 0};
  if(!rydb_data_append_cmd_rows(db, &commit_row, 1)) {
    return false;
  }
  return rydb_transaction_finish_or_continue(db, 1);
}

static bool rydb_transaction_start_or_continue_generic(rydb_t *db, int *transaction_started, uint_fast8_t oneshot) {
  if(db->transaction.active) {
    if(transaction_started) *transaction_started = 0;
    return true;
  };
  db->transaction.active = 1;
  if(oneshot) {
    db->transaction.oneshot = 1;
  }
  db->transaction.future_data_rownum = db->data_next_rownum;
  if(transaction_started) *transaction_started = 1;
  return true;
}
bool rydb_transaction_start_oneshot_or_continue(rydb_t *db, int *transaction_started) {
  return rydb_transaction_start_or_continue_generic(db, transaction_started, 1);
}
bool rydb_transaction_start_or_continue(rydb_t *db, int *transaction_started) {
  return rydb_transaction_start_or_continue_generic(db, transaction_started, 0);
}

bool rydb_transaction_start(rydb_t *db) {
  if(db->transaction.active) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_ACTIVE, "Transaction is already active");
    return false;
  }
  return rydb_transaction_start_or_continue(db, NULL);
}

bool rydb_transaction_cancel(rydb_t *db) {
  if(!db->transaction.active) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_INACTIVE, "No active transaction to stop");
    return false;
  }
  
  RYDB_REVERSE_EACH_CMD_ROW(db, cur) {
    cur->type = RYDB_ROW_EMPTY;
  }
  rydb_transaction_data_reset(db);
  db->cmd_next_rownum = db->data_next_rownum;
  return true;
}
