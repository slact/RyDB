#include "rydb_internal.h"
#include <string.h>
#include <assert.h>

static int rydb_cmd_rangecheck(rydb_t *db, const char *cmdname, rydb_stored_row_t *cmd, rydb_stored_row_t *dst) {
  if(!dst) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command %s [%i] failed: rownum out of range", cmdname, cmd->target_rownum);
    cmd->type = RYDB_ROW_EMPTY;
    return 0;
  }
  if(dst > cmd) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command %s [%i] failed: rownum is beyond command rownum", cmdname, cmd->target_rownum);
    cmd->type = RYDB_ROW_EMPTY;
    return 0;
  }
  return 1;
}

int rydb_data_append_cmd_rows(rydb_t *db, rydb_row_t *rows, const off_t count) {
  uint_fast16_t        rowsize = db->stored_row_size;
  rydb_stored_row_t   *newrows_start = db->cmd_next_row;
  rydb_stored_row_t   *newrows_end = rydb_row_next(newrows_start, rowsize, count);
  
  if(!rydb_file_ensure_writable_address(db, &db->data, newrows_start, ((char *)newrows_end - (char *)newrows_start))) {
    return 0;
  }
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
        return 0;
    }
    cur->target_rownum = rows[i].num;
    cur->type = rows[i].type;
    cur = rydb_row_next(cur, rowsize, 1);
  }
  db->cmd_next_row = newrows_end;
  return 1;
}

static inline int rydb_cmd_set(rydb_t *db, rydb_stored_row_t *cmd) {
  size_t               rowsz = db->stored_row_size;
  rydb_stored_row_t   *dst = rydb_rownum_to_row(db, cmd->target_rownum);
  if(!rydb_cmd_rangecheck(db, "SET", cmd, dst)) {
    return(0);
  }
  if(cmd != dst || dst->type != RYDB_ROW_EMPTY) {
    rydb_indices_remove_row(db, dst);
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
  rydb_indices_add_row(db, dst);
  if(dst >= db->data_next_row) {
    db->data_next_row = rydb_row_next(dst, rowsz, 1);
  }
  return 1;
}

static inline int rydb_cmd_update(rydb_t *db, rydb_stored_row_t *cmd) {
  rydb_row_cmd_header_t    *header = (rydb_row_cmd_header_t *)(void *)cmd->data;
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd->target_rownum);
  if(!rydb_cmd_rangecheck(db, "UPDATE", cmd, dst)) {
    return(0);
  }
  char *update_data = (char *)&header[1];
  rydb_indices_update_remove_row(db, dst, header->start, header->len);
  memcpy(&dst->data[header->start], update_data, header->len);
  rydb_indices_update_add_row(db, dst, header->start, header->len);
  cmd->type = RYDB_ROW_EMPTY;
  return 1;
}

static inline int rydb_cmd_update1(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd2 || cmd2->type != RYDB_ROW_CMD_UPDATE2) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command UPDATE2 [%i] failed: second command row has the wrong type or is missing");
    cmd1->type = RYDB_ROW_EMPTY;
    if(cmd2) {
      cmd2->type = RYDB_ROW_EMPTY;
    }
    return 0;
  }
  //do nothing if everything is ok
  return 1;
}
static inline int rydb_cmd_update2(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd1) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command UPDATE2 [%i] failed: one of the UPDATE2 rows is missing");
    cmd2->type = RYDB_ROW_EMPTY;
    return 0;
  }
  rydb_row_cmd_header_t    *header = (rydb_row_cmd_header_t *)(void *)cmd1->data;
  if(cmd1->type != RYDB_ROW_CMD_UPDATE1 || cmd2->type != RYDB_ROW_CMD_UPDATE2) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command UPDATE2 [%i] failed: one of the UPDATE rows has the wrong type");
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return 0;
  }
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd1->target_rownum);
  if(!rydb_cmd_rangecheck(db, "UPDATE2", cmd1, dst)) {
    return 0;
  }
  rydb_indices_update_remove_row(db, dst, header->start, header->len);
  memcpy(&dst->data[header->start], cmd2->data, header->len);
  cmd2->type = RYDB_ROW_EMPTY;
  cmd1->type = RYDB_ROW_EMPTY;
  rydb_indices_update_add_row(db, dst, header->start, header->len);
  return 1;
}
static inline int rydb_cmd_delete(rydb_t *db, rydb_stored_row_t *cmd) {
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd->target_rownum);
  if(!rydb_cmd_rangecheck(db, "DELETE", cmd, dst)) {
    return 0;
  }
  rydb_indices_remove_row(db, dst);
  dst->type = RYDB_ROW_EMPTY;
  cmd->type = RYDB_ROW_EMPTY;
  // remove contiguous empty rows at the end of the data from the data range
  // this gives the DELETE command a worst-case performance of O(n)
  // (but only when deleting the last row)
  rydb_data_update_last_nonempty_data_row(db, dst);
  return 1;
}

static inline int rydb_cmd_swap1(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd2) {//that's weird...
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: second command row is missing");
    cmd1->type = RYDB_ROW_EMPTY;
    return 0;
  }
  rydb_stored_row_t  *src, *dst;
  switch(cmd2->type) {
    case RYDB_ROW_CMD_SWAP2:
      // don't need to be doing anything.
      // SWAP2 needs to run first to change itself to a SET or DELETE
      // then it will run SWAP1
      return 1;
    case RYDB_ROW_CMD_SET:
    case RYDB_ROW_CMD_DELETE:
      //run the swap now, the temp src copy has already been written
      src = rydb_rownum_to_row(db, cmd1->target_rownum);
      dst = rydb_rownum_to_row(db, cmd2->target_rownum);
      if(!rydb_cmd_rangecheck(db, "SWAP", cmd1, src) || !rydb_cmd_rangecheck(db, "SWAP", cmd1, dst)) {
        return 0;
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
      return 1;
    default:
      rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed:  second command row has the wrong type");
      cmd1->type = RYDB_ROW_EMPTY;
      return 0;
  }
}

static inline int rydb_cmd_swap2(rydb_t *db, rydb_stored_row_t *cmd1, rydb_stored_row_t *cmd2) {
  if(!cmd1) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP2 failed: SWAP1 is missing");
    return 0;
  }
  rydb_stored_row_t  *src = rydb_rownum_to_row(db, cmd1->target_rownum);
  rydb_stored_row_t  *dst = rydb_rownum_to_row(db, cmd2->target_rownum);
  if(!rydb_cmd_rangecheck(db, "SWAP2", cmd1, src) || !rydb_cmd_rangecheck(db, "SWAP2", cmd1, dst)) {
    return 0;
  }
  
  //TODO: figure out efficient implementation for reindexing swaps
  
  size_t              rowsz = db->stored_row_size;
  
  if(cmd2->type != RYDB_ROW_CMD_SWAP2 || cmd1->type != RYDB_ROW_CMD_SWAP1) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: one of the command rows is the wrong type");
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return 0;
  }
  
  rydb_row_type_t srctype = src->type, dsttype = dst->type;
  if(dsttype != RYDB_ROW_EMPTY && dsttype != RYDB_ROW_DATA) {
        rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Command SWAP failed: dst row is the wrong type");
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return 0;
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
      return 0;
  }
  if(!rydb_cmd_swap1(db, cmd1, cmd2)) {
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return 0;
  }
  int rc = 0;
  if(cmd2type == RYDB_ROW_CMD_SET) {
    rc = rydb_cmd_set(db, cmd2);
  }
  else if(cmd2type == RYDB_ROW_CMD_DELETE) {
    rc = rydb_cmd_delete(db, cmd2);
  }
  if(!rc) {
    cmd1->type = RYDB_ROW_EMPTY;
    cmd2->type = RYDB_ROW_EMPTY;
    return 0;
  }
  return 1;
}

int rydb_transaction_run(rydb_t *db, rydb_stored_row_t *last_row_to_run) {
  rydb_stored_row_t *prev = NULL, *next;
  rydb_stored_row_t *lastcmd;
  if(last_row_to_run) {
    lastcmd = last_row_to_run;
  }
  else {
    lastcmd = rydb_row_next(db->cmd_next_row, db->stored_row_size, -1);
  }
  if(lastcmd < db->data_next_row || 
    (rydb_debug_refuse_to_run_transaction_without_commit && lastcmd->type != RYDB_ROW_CMD_COMMIT)) {
    // no CMD_COMMIT at the end -- bail
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_INCOMPLETE, "Refused to run a transaction that doesn't end with a COMMIT");
    return 0;
  }
  int rc = 1;
  rydb_stored_row_t *commit_row = NULL;
  RYDB_EACH_CMD_ROW(db, cur) {
    if(last_row_to_run && cur > last_row_to_run) {
      break;
    }
    else if(rc) {
      switch((rydb_row_type_t )cur->type) {
        case RYDB_ROW_EMPTY:
        case RYDB_ROW_DATA:
          //do nothing, these are weird.
          break;
        case RYDB_ROW_CMD_SET:
          rc = rydb_cmd_set(db, cur);
          break;
        case RYDB_ROW_CMD_UPDATE:
          rc = rydb_cmd_update(db, cur);
          break;
        case RYDB_ROW_CMD_UPDATE1:
          next = rydb_row_next(cur, db->stored_row_size, 1);
          if(next >= db->cmd_next_row) next = NULL;
          rc = rydb_cmd_update1(db, cur, next);
          break;
        case RYDB_ROW_CMD_UPDATE2:
          rc = rydb_cmd_update2(db, prev, cur);
          break;
        case RYDB_ROW_CMD_DELETE:
          rc = rydb_cmd_delete(db, cur);
          break;
        case RYDB_ROW_CMD_SWAP1:
          next = rydb_row_next(cur, db->stored_row_size, 1);
          if(next >= db->cmd_next_row) next = NULL;
          rc = rydb_cmd_swap1(db, cur, next);
          break;
        case RYDB_ROW_CMD_SWAP2:
          rc = rydb_cmd_swap2(db, prev, cur);
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
  if(!rc) {
    return 0;
  }
  if(commit_row != prev) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_FAILED, "Transaction was committed without ending on a COMMIT command");
    return 0;
  }
  return rc;
}

int rydb_transaction_finish_or_continue(rydb_t *db, int finish) {
  if(finish && db->transaction.active) {
    int rc = rydb_transaction_run(db, NULL);
    //succeed or fail -- the transaction should be cleared
    db->transaction.active = 0;
    db->cmd_next_row = db->data_next_row;
    return rc;
  }
  return 1;
}

int rydb_transaction_finish(rydb_t *db) {
  if(!db->transaction.active) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_INACTIVE, "No active transaction to finish");
    return 0;
  }
  rydb_row_t commit_row = {.type = RYDB_ROW_CMD_COMMIT, .num = 0};
  if(!rydb_data_append_cmd_rows(db, &commit_row, 1)) {
    return 0;
  }
  return rydb_transaction_finish_or_continue(db, 1);
}

int rydb_transaction_start_or_continue(rydb_t *db, int *transaction_started) {
  if(db->transaction.active) {
    if(transaction_started) *transaction_started = 0;
    return 1;
  };
  db->transaction.active = 1;
  db->transaction.future_data_rownum = rydb_row_to_rownum(db, db->data_next_row);
  if(transaction_started) *transaction_started = 1;
  return 1;
}

int rydb_transaction_start(rydb_t *db) {
  if(db->transaction.active) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_ACTIVE, "Transaction is already active");
    return 0;
  }
  return rydb_transaction_start_or_continue(db, NULL);
}

int rydb_transaction_cancel(rydb_t *db) {
  if(!db->transaction.active) {
    rydb_set_error(db, RYDB_ERROR_TRANSACTION_INACTIVE, "No active transaction to stop");
    return 0;
  }
  
  RYDB_REVERSE_EACH_CMD_ROW(db, cur) {
    cur->type = RYDB_ROW_EMPTY;
  }
  db->cmd_next_row = db->data_next_row;
  db->transaction.active = 0;
  return 1;
}
