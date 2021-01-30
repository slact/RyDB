# RyDB

[![coverage](https://img.shields.io/codecov/c/github/slact/RyDB/master.svg)](https://codecov.io/gh/slact/RyDB)
[![build status](https://api.cirrus-ci.com/github/slact/RyDB.svg)](https://cirrus-ci.com/github/slact/RyDB)

RyDB is a high-performance, fixed-size row database built as a C library for applications requiring predictable data access patterns. It can be embedded in systems, used as a caching layer, or deployed as persistent storage where data structures are known in advance. RyDB uses memory-mapped files for zero-copy I/O, hashtable indices for O(1) lookups, and provides ACD transactions (Atomicity, Consistency, Durability) through an innovative [zero-copy command log](#command-log). Transaction isolation is traded for zero-copy performance.

Rows are stored in [fixed-size records](#configuration) with configurable [hashtable indexing](#index-configuration), and accessed via [direct operations](#data-operations) or [index queries](#cursors-and-iteration). All modifications use [ACID transactions](#transactions) with an integrated [command log](#command-log) that eliminates data copying by treating commands as temporary rows. The system supports [row relationships](#row-links) and enforces [unique constraints](#error-handling) automatically.

## Features

- **[Fixed-size rows](#configuration)** with configurable length for predictable memory usage and performance
- **[Inline command log](#command-log)** that eliminates data copying during transaction commits
- **[Zero-Copy design](#zero-copy-insert-design)** for insertions
- **[Multiple possible index types](#index-configuration)** including hashtables with configurable collision resolution strategy, functions, and growth handling
- **[ACD transactions (No I for now)](#transactions)**
- **[Memory-mapped file I/O](#file-structure)** for efficient data access and persistence
- **[Concurrent access](#concurrency)** with single writer and many readers
- **[Direct Row linking](#row-links)** system for creating relationships between records
- **[Unique constraints](#error-handling) for indices** with automatic validation

## Quick Start

### Building from Source

```bash
# Clone the repository
git clone https://github.com/slact/RyDB.git
cd RyDB

# Build with CMake

cmake -B build

cmake --build build

build/test
```

### Basic Usage

```c
#include <rydb.h>

int main() {
    // Create and configure database
    rydb_t *db = rydb_new();
    rydb_config_row(db, 64, 16);  // 64-byte rows, 16-byte primary key
    
    // Add a hashtable index
    rydb_config_add_index_hashtable(db, "primary", 0, 16, RYDB_INDEX_UNIQUE, NULL);
    
    // Open database
    rydb_open(db, "/path/to/db", "mydb");
    
    // Insert data
    char data[64] = "user123         John Doe, age 30, email: john@example.com";
    rydb_insert(db, data, 64);
    
    // Find data using the primary index
    rydb_row_t row;
    if (rydb_find_row_str(db, "user123", &row)) {
        printf("Found: %s\n", row.data);
    }
    
    // Clean up
    rydb_close(db);
    return 0;
}
```

## Configuration

### Row Configuration

Configure the database schema before opening:

```c
rydb_t *db = rydb_new();

// Set row length and primary key length
rydb_config_row(db, 256, 32);  // 256-byte rows, 32-byte keys

// Set database revision for schema versioning
rydb_config_revision(db, 1);
```

### Primary Key Configuration

The `id_len` parameter in `rydb_config_row(db, row_len, id_len)` controls automatic primary key creation:

```c
// Automatic primary key at offset 0
rydb_config_row(db, 256, 32);  // Creates "primary" index: bytes 0-31
rydb_open(db, path, name);      // Primary index created automatically

// No automatic primary key
rydb_config_row(db, 256, 0);   // id_len = 0, no automatic index
rydb_open(db, path, name);      // No primary index created
```

- When `id_len > 0`: A unique index named "primary" is automatically created at offset 0 with length `id_len`
- When `id_len = 0`: No automatic primary index is created

You can manually create a primary key at any offset:

```c
rydb_t *db = rydb_new();
rydb_config_row(db, 256, 0);  // No automatic primary key

// Create primary key of length 32 starting at offset 64
rydb_config_add_index_hashtable(db, "primary", 64, 32, RYDB_INDEX_UNIQUE, NULL);

rydb_open(db, path, name);
```

Note that the primary index **must have the name "primary"**

### Index Configuration

RyDB supports hashtable indices with various configuration options:

```c
// Basic unique index (primary key)
rydb_config_add_index_hashtable(db, "primary", 0, 32, RYDB_INDEX_UNIQUE, NULL);

// Advanced hashtable configuration
rydb_config_index_hashtable_t config = {
    .hash_function = RYDB_HASH_SIPHASH,     // SipHash, CRC32, or NOHASH
    .collision_resolution = RYDB_OPEN_ADDRESSING,
    .load_factor_max = 0.75,
    .store_value = 1,                        // Store values in index
    .store_hash = 1,                         // Store hash values
    .rehash = RYDB_REHASH_INCREMNTAL       // Rehashing strategy
};

rydb_config_add_index_hashtable(db, "secondary", 32, 16, RYDB_INDEX_DEFAULT, &config);
```

### Row Links

Create relationships between rows:

```c
// Add bidirectional links
rydb_config_add_row_link(db, "next", "prev");
rydb_config_add_row_link(db, "parent", "child");
```

### Using Row Links

Once configured, you can set and follow links between rows:

```c
rydb_row_t row1, row2, linked_row;

// Find rows to link
rydb_find_row_str(db, "foo", &row_foo);
rydb_find_row_str(db, "bar", &row_bar);

// Create bidirectional link
rydb_row_set_link(db, &row_foo, "next", &row_bar);
// This automatically sets row_bar's "prev" link to row_foo

// Follow links
if (rydb_row_get_link(db, &row_foo, "next", &linked_row)) {
    //linked_row is now set to row_bar
    printf("Next item: %s\n", linked_row.data);
}

// Link by row number
rydb_row_set_link_rownum(db, &row1, "parent", 42);
```

## Data Operations

### Inserting Data

```c
// Insert string data
rydb_insert_str(db, "key123      Some data here...");

// Insert binary data with specific length
char data[256];
memset(data, 0, 256);
strcpy(data, "key456");
// ... fill rest of data ...
rydb_insert(db, data, 256);
```

### Finding Data

```c
rydb_row_t row;

// Find by primary key
if (rydb_find_row_str(db, "key123", &row)) {
    printf("Row %d: %s\n", row.num, row.data);
}

// Find by secondary index
if (rydb_index_find_row_str(db, "secondary", "search_value", &row)) {
    printf("Found via secondary index: %s\n", row.data);
}

// Find by row number
if (rydb_find_row_at(db, 42, &row)) {
    printf("Row 42: %s\n", row.data);
}
```

### Updating Data

```c
// Update part of a row
rydb_update_rownum(db, row_number, "new_data", start_offset, length);

// Update entire row
rydb_update_rownum(db, row_number, new_data, 0, row_length);
```

### Deleting Data

```c
// Delete by row number
rydb_delete_rownum(db, row_number);

// Swap two rows
rydb_swap_rownum(db, row1, row2);
```

## Transactions

RyDB suports ACD (no I) transactions for atomic operations:

```c
// Start transaction
rydb_transaction_start(db);

// Perform multiple operations
rydb_insert_str(db, "key1        data1");
rydb_insert_str(db, "key2        data2");
rydb_update_rownum(db, 1, "updated", 12, 7);

// Commit transaction
rydb_transaction_finish(db);

// Or rollback if needed
// rydb_transaction_cancel(db);
```

### Constraint Validation Before Commit

Unique constraints are checked before commands are written to the log. Each operation validates against both existing indices and in-memory transaction state, and constraint violations prevent the operation from being added to the transaction.

### Crash Recovery

On database open, RyDB scans for uncommitted transactions. Transaction commands without a COMMIT command are discarded, while transactions with a COMMIT command are replayed idempotently. The command log is truncated after recovery to reclaim space.

### Durability Guarantees

All commands are idempotent and can be safely re-executed, with crashed transactions automatically rolled back or complieted on next open -- dependent on whether they were committed.

### **No** Transaction Isolation **

Readers may see partial transaction state during command execution, as commands become visible immediately when executed rather than atomically. This trades isolation for zero-copy performance and direct memory access. A future isolated read mode is planned to provide optional ACID compliance.

## Command Log

RyDB uses an integrated command log that eliminates data copying during transactions. Instead of a separate write-ahead log, commands are appended as regular rows to the main data file.

### Zero-Copy Insert Design

In a traditional write-ahead log, data is written to the log, then copied to main storage during commit.
The RyDB approach is to INSERT commands contain the actual row data. During commit, the command row simply changes its type from `CMD_SET` to `ROW_DATA` -- no copying required. That is, committing a transaction for inserting data requires no more than writing a single byte per committed row.

```c
// Command appended during transaction (for insert):
{ type: RYDB_ROW_CMD_SET, data: "actual_row_data..." }

// After commit, same memory becomes:
{ type: RYDB_ROW_DATA, data: "actual_row_data..." }
```

### Why This Works

- **Fixed-size rows**: Commands  and data use identical storage layout
- **In-place conversion**: Type field change converts command to data
- **Memory-mapped**: No system calls needed for the conversion
- **Atomic**: Single byte write (type field) provides atomicity

This design leverages teh fixed-size constraint to eliminate the traditional WAL copy overhead, making inserts essentially free during commit.

## Cursors and Iteration

Collections of rows are accessed via cursors.

### Index Cursors

```c
rydb_cursor_t cursor;

// Find all rows matching a key in a non-unique index
if (rydb_index_find_rows_str(db, "category", "electronics", &cursor)) {
    rydb_row_t row;
    while (rydb_cursor_next(&cursor, &row)) {
        printf("Row %d: %s\n", row.num, row.data);
    }
}
```

### Data Cursors

These are used for traversing an entire table roughly in insertion order.

```c
rydb_cursor_t cursor;

// Iterate through all data rows
if (rydb_rows(db, &cursor)) {
    rydb_row_t row;
    while (rydb_cursor_next(&cursor, &row)) {
        if (row.type == RYDB_ROW_DATA) {
            printf("Row %d: %s\n", row.num, row.data);
        }
    }
}
```

## Index Management

RyDB currently provided one typoe of index -- a highly configurable hashtable.

### Hash Index

### Hashtable Rehashing

```c
// Manual rehash of a hashtable index
rydb_index_rehash(db, "index_name");
```

### Hash Functions

RyDB supports multiple hash functions:

- **RYDB_HASH_SIPHASH**: Cryptographically secure, good distribution
- **RYDB_HASH_CRC32**: Fast, good for non-adversarial data
- **RYDB_HASH_NOHASH**: Treats input as pre-hashed

### Collision Resolution

- **RYDB_OPEN_ADDRESSING**: Linear probing, cache-friendly
- **RYDB_SEPARATE_CHAINING**: Linked lists, handles high load factors

## Error Handling

Failing commands return `false`, and an error string is set explaining the failure. It can be accessed as follows:

```c
// Check for errors after operations
if (!rydb_insert_str(db, "data")) {
    rydb_error_t *error = rydb_error(db);
    if (error) {
        printf("Error: %s\n", error->str);
        rydb_error_clear(db);
    }
}

Additionally custom error handling is available ot make error printing less verbose.

// Set custom error handler
void my_error_handler(rydb_t *db, rydb_error_t *err, void *userdata) {
    fprintf(stderr, "RyDB Error: %s\n", err->str);
}

rydb_set_error_handler(db, my_error_handler, NULL);
```

## Concurrency

RyDB supports concurrent access with proper locking:

```c
// Open for read-write (exclusive)
rydb_open(db, path, name);

// Open for read-only (shared)
rydb_open_reader(db, path, name);

// Force unlock (for recovery from a crash)
rydb_force_unlock(db);
```

## File Structure

RyDB creates several files for each database:

- `rydb.name.data` - Main data file with row storage
- `rydb.name.meta` - Metadata and configuration
- `rydb.name.state` - Runtime state and locks
- `rydb.name.index.*` - Index files for each defined index

## Performance Considerations

### Memory Usage

- Fixed-size rows provide predictable memory usage
- Memory-mapped I/O reduces system call overhead
- Configurable index storage (values and/or hashes)

### Hashtable Indexing Strategy

- Use unique indices for primary keys
- Consider load factor vs. performance trade-offs
- Choose appropriate hash functions for your data
- Incremental rehashing is preferrable for large datasets

### Transaction Overhead

- There isn't any. Because of the in-place command log, transactions cost no more than regular operations.
  In fact, regular operations translate internally to single-command transactions.

## Limitations

- Fixed row size must be determined at database creation
- Maximum row size is 65Kb
- Maximum of 32 indices per database
- Single writer, multiple reader concurrency model
- Transactions are not wholly ACID, but rather ACD. The "I" (Isolation) is coming later.

## API Reference

See `src/rydb.h` for API and data structures.

## TODO

### Space Reclamation

RyDB currently creates permanent holes when rows are deleted, leading to space fragmentation. Planned solution: extend the zero-copy command log to reuse freed space by placing commands in holes instead of only appending at teh end. This preserves zero-copy performance but trades storage fragmentation for access fragmentation.

### Isolated-Transaction Read Mode

Add optional read mode that traeds zero-copy read semantics for transaction isolation by simulating committed transaction effects on returned data. This would provide consistent snapshots for applications requiring full ACID compliance.

### Row-Level read/write "locks"

To preserve zero-copy writes, reads can be done into a buffered copy when a row is about to be modified by a transaction that has been committed but not yet written.
