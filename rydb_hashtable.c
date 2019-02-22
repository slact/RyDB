#include "rydb_internal.h"
#include "rydb_hashtable.h"
#include <stdlib.h>
#include <stdint.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>


/**
 * \file pycrc_stdout
 * Functions and types for CRC checks.
 *
 * Generated on Thu Feb 14 23:59:19 2019,
 * by pycrc v0.9, https://pycrc.org
 * using the configuration:
 *    Width         = 32
 *    Poly          = 0x04c11db7
 *    Xor_In        = 0xffffffff
 *    ReflectIn     = True
 *    Xor_Out       = 0xffffffff
 *    ReflectOut    = True
 *    Algorithm     = table-driven
 *****************************************************************************/

#define CRC_ALGO_TABLE_DRIVEN 1
typedef uint_fast32_t crc_t;
static inline crc_t crc_init(void) {
  return 0xffffffff;
}
static inline crc_t crc_finalize(crc_t crc) {
  return crc ^ 0xffffffff;
}

/**
 * Static table used for the table_driven implementation.
 *****************************************************************************/
static const crc_t crc_table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
    0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988, 0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
    0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
    0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172, 0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
    0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
    0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
    0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
    0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e, 0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
    0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
    0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0, 0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
    0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
    0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a, 0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
    0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
    0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc, 0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
    0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
    0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
    0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
    0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
    0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
    0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
    0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
    0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

/**
 * Update the crc value with new data.
 *
 * \param crc      The current crc value.
 * \param data     Pointer to a buffer of \a data_len bytes.
 * \param data_len Number of bytes in the \a data buffer.
 * \return         The updated crc value.
 *****************************************************************************/
static crc_t crc_update(crc_t crc, const void *data, size_t data_len)
{
    const unsigned char *d = (const unsigned char *)data;
    unsigned int tbl_idx;

    while (data_len--) {
        tbl_idx = (crc ^ *d) & 0xff;
        crc = (crc_table[tbl_idx] ^ (crc >> 8)) & 0xffffffff;

        d++;
    }
    return crc & 0xffffffff;
}


uint64_t crc32(const uint8_t *data, size_t data_len) {
  crc_t crc = crc_init();
  crc = crc_update(crc, data, data_len);
  crc = crc_finalize(crc);
  return crc;
}

/*
   SipHash reference C implementation
   Copyright (c) 2012-2016 Jean-Philippe Aumasson
   <jeanphilippe.aumasson@gmail.com>
   Copyright (c) 2012-2014 Daniel J. Bernstein <djb@cr.yp.to>
   Copyright (c) 2017 Salvatore Sanfilippo <antirez@gmail.com>
   To the extent possible under law, the author(s) have dedicated all copyright
   and related and neighboring rights to this software to the public domain
   worldwide. This software is distributed without any warranty.
   You should have received a copy of the CC0 Public Domain Dedication along
   with this software. If not, see
   <http://creativecommons.org/publicdomain/zero/1.0/>.
   ----------------------------------------------------------------------------
   This version was modified by Salvatore Sanfilippo <antirez@gmail.com>
   in the following ways:
   1. Hard-code 2-4 rounds in the hope the compiler can optimize it more
      in this raw from. Anyway we always want the standard 2-4 variant.
   2. Modify the prototype and implementation so that the function directly
      returns an uint64_t value, the hash itself, instead of receiving an
      output buffer. This also means that the output size is set to 8 bytes
      and the 16 bytes output code handling was removed.
   3. Provide a case insensitive variant to be used when hashing strings that
      must be considered identical by the hash table regardless of the case.
      If we don't have directly a case insensitive hash function, we need to
      perform a text transformation in some temporary buffer, which is costly.
   4. Remove debugging code.
   5. Modified the original test.c file to be a stand-alone function testing
      the function in the new form (returing an uint64_t) using just the
      relevant test vector.
 */

/* Test of the CPU is Little Endian and supports not aligned accesses.
 * Two interesting conditions to speedup the function that happen to be
 * in most of x86 servers. */

#if defined(__X86_64__) || defined(__x86_64__) || defined (__i386__)
#define UNALIGNED_LE_CPU
#endif

#define ROTL(x, b) (uint64_t)(((x) << (b)) | ((x) >> (64 - (b))))

#define U32TO8_LE(p, v)                                                        \
  (p)[0] = (uint8_t)((v));                                                   \
  (p)[1] = (uint8_t)((v) >> 8);                                              \
  (p)[2] = (uint8_t)((v) >> 16);                                             \
  (p)[3] = (uint8_t)((v) >> 24);

#define U64TO8_LE(p, v)                                                        \
  U32TO8_LE((p), (uint32_t)((v)));                                           \
  U32TO8_LE((p) + 4, (uint32_t)((v) >> 32));

#ifdef UNALIGNED_LE_CPU
#define U8TO64_LE(p) (*((uint64_t*)(p)))
#else
#define U8TO64_LE(p)                                                           \
  (((uint64_t)((p)[0])) | ((uint64_t)((p)[1]) << 8) |                        \
    ((uint64_t)((p)[2]) << 16) | ((uint64_t)((p)[3]) << 24) |                 \
    ((uint64_t)((p)[4]) << 32) | ((uint64_t)((p)[5]) << 40) |                 \
    ((uint64_t)((p)[6]) << 48) | ((uint64_t)((p)[7]) << 56))
#endif

#define SIPROUND                                                               \
  do {                                                                       \
    v0 += v1;                                                              \
    v1 = ROTL(v1, 13);                                                     \
    v1 ^= v0;                                                              \
    v0 = ROTL(v0, 32);                                                     \
    v2 += v3;                                                              \
    v3 = ROTL(v3, 16);                                                     \
    v3 ^= v2;                                                              \
    v0 += v3;                                                              \
    v3 = ROTL(v3, 21);                                                     \
    v3 ^= v0;                                                              \
    v2 += v1;                                                              \
    v1 = ROTL(v1, 17);                                                     \
    v1 ^= v2;                                                              \
    v2 = ROTL(v2, 32);                                                     \
  } while (0)

  
#if defined(__GNUC__) && !defined(__clang__) //don't warn about implicit fallthrough
#define SWITCHLEFT_CASE(n, b, in) \
  case n: b |= ((uint64_t)in[n-1]) << (8*(n-1)); __attribute__((fallthrough));
#else
#define SWITCHLEFT_CASE(n, b, in) \
  case n: b |= ((uint64_t)in[n-1]) << (8*(n-1));
#endif
    
uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k) {
#ifndef UNALIGNED_LE_CPU
  uint64_t hash;
  uint8_t *out = (uint8_t*) &hash;
#endif
  uint64_t v0 = 0x736f6d6570736575ULL;
  uint64_t v1 = 0x646f72616e646f6dULL;
  uint64_t v2 = 0x6c7967656e657261ULL;
  uint64_t v3 = 0x7465646279746573ULL;
  uint64_t k0 = U8TO64_LE(k);
  uint64_t k1 = U8TO64_LE(k + 8);
  uint64_t m;
  const uint8_t *end = in + inlen - (inlen % sizeof(uint64_t));
  const int left = inlen & 7;
  uint64_t b = ((uint64_t)inlen) << 56;
  v3 ^= k1;
  v2 ^= k0;
  v1 ^= k1;
  v0 ^= k0;

  for (; in != end; in += 8) {
    m = U8TO64_LE(in);
    v3 ^= m;

    SIPROUND;
    SIPROUND;

    v0 ^= m;
  }

  switch (left) {
    SWITCHLEFT_CASE(7, b, in);
    SWITCHLEFT_CASE(6, b, in);
    SWITCHLEFT_CASE(5, b, in);
    SWITCHLEFT_CASE(4, b, in);
    SWITCHLEFT_CASE(3, b, in);
    SWITCHLEFT_CASE(2, b, in);
    case 1: b |= ((uint64_t)in[0]); break;
    case 0: break;
  }

  v3 ^= b;

  SIPROUND;
  SIPROUND;

  v0 ^= b;
  v2 ^= 0xff;

  SIPROUND;
  SIPROUND;
  SIPROUND;
  SIPROUND;

  b = v0 ^ v1 ^ v2 ^ v3;
#ifndef UNALIGNED_LE_CPU
  U64TO8_LE(out, b);
  return hash;
#else
  return b;
#endif
}

int rydb_meta_load_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp) {
  const char *fmt =
    "    hash_function: %32s\n"
    "    store_value: %"SCNu16"\n"
    "    direct_mapping: %"SCNu16"\n"
    "    load_factor_max: %f\n";

  char      hash_func_buf[33];
  uint16_t  store_value;
  uint16_t  direct_mapping;
  float     load_factor_max;

  int rc = fscanf(fp, fmt, hash_func_buf, &store_value, &direct_mapping, &load_factor_max);
  if(rc < 4 || store_value > 1 || direct_mapping > 1 || load_factor_max >= 1 || load_factor_max <= 0) {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Hashtable \"%s\" specification is corrupted or invalid", idx_cf->name);
    return 0;
  }
  if(strcmp("CRC32", hash_func_buf) == 0) {
    idx_cf->type_config.hashtable.hash_function = RYDB_HASH_CRC32;
  }
  else if(strcmp("none", hash_func_buf) == 0) {
    idx_cf->type_config.hashtable.hash_function = RYDB_HASH_NOHASH;
  }
  else if(strcmp("SipHash", hash_func_buf) == 0) {
    idx_cf->type_config.hashtable.hash_function = RYDB_HASH_SIPHASH;
  }
  else {
    rydb_set_error(db, RYDB_ERROR_FILE_INVALID, "Unsupported hash function %s for hashtable \"%s\"", hash_func_buf, idx_cf->name);
    return 0;
  }
  idx_cf->type_config.hashtable.store_value = store_value;
  idx_cf->type_config.hashtable.direct_mapping = direct_mapping;
  idx_cf->type_config.hashtable.load_factor_max = load_factor_max;
  return 1;
}
static char *hashfunction_to_str(rydb_config_index_type_t *cf) {
  switch(cf->hashtable.hash_function) {
    case RYDB_HASH_CRC32:
      return "CRC32";
    case RYDB_HASH_NOHASH:
      return "none";
    case RYDB_HASH_SIPHASH:
      return "SipHash";
    case RYDB_HASH_INVALID:
      return "invalid";
  }
  return "???";
}
static int hashfunction_valid(rydb_config_index_hashtable_t *acf) {
  switch(acf->hash_function) {
    case RYDB_HASH_INVALID:
      return 0;
    case RYDB_HASH_CRC32:
    case RYDB_HASH_NOHASH:
    case RYDB_HASH_SIPHASH:
      return 1;
  }
  return 0;
}

int rydb_meta_save_index_hashtable(rydb_t *db, rydb_config_index_t *idx_cf, FILE *fp) {
  const char *fmt =
    "    hash_function: %s\n"
    "    store_value: %"PRIu16"\n"
    "    direct_mapping: %"PRIu16"\n"
    "    load_factor_max: %.4f\n";
  int rc;
  rc = fprintf(fp, fmt, hashfunction_to_str(&idx_cf->type_config), (uint16_t )idx_cf->type_config.hashtable.store_value, (uint16_t )idx_cf->type_config.hashtable.direct_mapping, idx_cf->type_config.hashtable.load_factor_max);
  if(rc <= 0) {
    rydb_set_error(db, RYDB_ERROR_FILE_ACCESS, "failed writing hashtable \"%s\" config ", idx_cf->name);
    return 0;
  }
  return 1;
}

int rydb_config_index_hashtable_set_config(rydb_t *db, rydb_config_index_t *cf, rydb_config_index_hashtable_t *advanced_config) {
  unsigned unique = cf->flags & RYDB_INDEX_UNIQUE;
  if(!advanced_config) {
    cf->type_config.hashtable.direct_mapping = unique;
    cf->type_config.hashtable.store_value = !unique;
    cf->type_config.hashtable.hash_function = RYDB_HASH_SIPHASH;
    cf->type_config.hashtable.load_factor_max = RYDB_HASHTABLE_DEFAULT_MAX_LOAD_FACTOR;
  }
  else if(!hashfunction_valid(advanced_config)) {
    rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid hash function for hashtable \"%s\" config ", cf->name);
    return 0;
  }
  else {
    if(advanced_config->load_factor_max >= 1 || advanced_config->load_factor_max < 0) {
      rydb_set_error(db, RYDB_ERROR_BAD_CONFIG, "Invalid load_factor_max value %f for hashtable \"%s\", must be between 0 and 1", cf->name);
    return 0;
    }
    cf->type_config.hashtable = *advanced_config;
    if(cf->type_config.hashtable.load_factor_max == 0) {
      cf->type_config.hashtable.load_factor_max = RYDB_HASHTABLE_DEFAULT_MAX_LOAD_FACTOR;
    }
  }
  return 1;
}

static uint64_t nohash(const char *data, const size_t len, const uint8_t trim) {
  uint64_t h = 0;
  assert(trim <= 64);
  memcpy((char *)&h, data, (len > (size_t )(64-trim) ? (size_t )(64-trim) : len));
  return h;
}



static const uint64_t btrim64_mask[65] = {
  0xffffffffffffffff, 0x7fffffffffffffff, 0x3fffffffffffffff, 0x1fffffffffffffff,
  0x0fffffffffffffff, 0x07ffffffffffffff, 0x03ffffffffffffff, 0x01ffffffffffffff,
  0x00ffffffffffffff, 0x007fffffffffffff, 0x003fffffffffffff, 0x001fffffffffffff,
  0x000fffffffffffff, 0x0007ffffffffffff, 0x0003ffffffffffff, 0x0001ffffffffffff,
  0x0000ffffffffffff, 0x00007fffffffffff, 0x00003fffffffffff, 0x00001fffffffffff,
  0x00000fffffffffff, 0x000007ffffffffff, 0x000003ffffffffff, 0x000001ffffffffff,
  0x000000ffffffffff, 0x0000007fffffffff, 0x0000003fffffffff, 0x0000001fffffffff,
  0x0000000fffffffff, 0x00000007ffffffff, 0x00000003ffffffff, 0x00000001ffffffff,
  0x00000000ffffffff, 0x000000007fffffff, 0x000000003fffffff, 0x000000001fffffff,
  0x000000000fffffff, 0x0000000007ffffff, 0x0000000003ffffff, 0x0000000001ffffff,
  0x0000000000ffffff, 0x00000000007fffff, 0x00000000003fffff, 0x00000000001fffff,
  0x00000000000fffff, 0x000000000007ffff, 0x000000000003ffff, 0x000000000001ffff,
  0x000000000000ffff, 0x0000000000007fff, 0x0000000000003fff, 0x0000000000001fff,
  0x0000000000000fff, 0x00000000000007ff, 0x00000000000003ff, 0x00000000000001ff,
  0x00000000000000ff, 0x000000000000007f, 0x000000000000003f, 0x000000000000001f,
  0x000000000000000f, 0x0000000000000007, 0x0000000000000003, 0x0000000000000001, 0
};
//bitwise trim of a 64-bit uint from the big end
static inline uint64_t btrim64(uint64_t val, uint8_t bits) {
  return val & btrim64_mask[bits];
}


static uint64_t hash_value(rydb_t *db, rydb_config_index_t *idx, char *data, uint8_t trim) {
  uint64_t h;
  switch(idx->type_config.hashtable.hash_function) {
    case RYDB_HASH_CRC32:
      h = btrim64(crc32((uint8_t *)&data[idx->start], idx->len), trim);
      break;
    case RYDB_HASH_NOHASH:
      h = nohash(&data[idx->start], idx->len, trim);
      break;
    case RYDB_HASH_SIPHASH:
      h = btrim64(siphash((uint8_t *)&data[idx->start], idx->len, db->config.hash_key.value), trim);
      break;
    default:
    case RYDB_HASH_INVALID:
      h=0;
      break;
  }
  return h;
}

static inline size_t hashtable_entry_size(rydb_config_index_t *cf) {
  if(cf->type_config.hashtable.direct_mapping) {
    return sizeof(rydb_rownum_t);
  }
  else {
    return ry_align(sizeof(rydb_rownum_t) + cf->len, sizeof(rydb_rownum_t));
  }
}

static inline rydb_hashtable_header_t *hashtable_header(rydb_index_t *idx) {
  return (void *)idx->index.file.start;
}

static inline void hashtable_reserve(rydb_hashtable_header_t *header) {
  assert(header->reserved < INT8_MAX);
  header->reserved++;
}
static inline void hashtable_release(rydb_hashtable_header_t *header) {
  header->reserved--;
  assert(header->reserved >= 0);
}

static void hashtable_bitlevel_subtract(rydb_hashtable_header_t *header, int level) {
  if(--header->bucket.bitlevel.sub[level].count == 0) {
    for(int i =  level+1; i < header->bucket.count.sub_bitlevels; i++) {
      header->bucket.bitlevel.sub[i] = header->bucket.bitlevel.sub[i+1];
    }
    header->bucket.count.sub_bitlevels--;
    assert(header->bucket.bitlevel.sub[header->bucket.count.sub_bitlevels].count == 0);
    assert(header->bucket.bitlevel.sub[header->bucket.count.sub_bitlevels].bits == 0);
  }
}

static const rydb_hashtable_bitlevel_count_t *hashtable_bitlevel_next(const rydb_hashtable_header_t *header, const rydb_hashtable_bitlevel_count_t *bitlevel) {
  if(bitlevel == NULL) {
    return &header->bucket.bitlevel.top;
  }
  const rydb_hashtable_bitlevel_count_t *end = &header->bucket.bitlevel.sub[header->bucket.count.sub_bitlevels];
  if(bitlevel < end) {
    return &bitlevel[1];
  }
  return NULL;
}

static void hashtable_bitlevel_push(rydb_hashtable_header_t *header) {
  for(int i = header->bucket.count.sub_bitlevels - 1; i >= 0; i--) {
    header->bucket.bitlevel.sub[i+1] = header->bucket.bitlevel.sub[i];
  }
  header->bucket.bitlevel.sub[0] = header->bucket.bitlevel.top;
  header->bucket.count.sub_bitlevels++;
}

static int hashtable_grow(rydb_t *db, rydb_index_t *idx) {
  rydb_hashtable_header_t *header = hashtable_header(idx);
  rydb_config_index_t *cf = idx->config;
  hashtable_reserve(header);
  if(header->bucket.bitlevel.top.bits != 0) {
    hashtable_bitlevel_push(header);
  }
  header->bucket.bitlevel.top.bits++;
  header->bucket.bitlevel.top.count = 0;
  
  uint64_t max_bucket = (1 << header->bucket.bitlevel.top.bits);
  header->bucket.count.load_factor_max = max_bucket * cf->type_config.hashtable.load_factor_max;
  size_t sz = max_bucket * hashtable_entry_size(cf);
  header->bucket.count.total = max_bucket;
  hashtable_release(header);
  int rc = rydb_file_ensure_size(db, &idx->index, RYDB_INDEX_HASHTABLE_START_OFFSET + sz);
  if(rc) {
    idx->index.data.end = idx->index.file.end;
  }
  return rc;
}

int rydb_index_hashtable_open(rydb_t *db,  rydb_index_t *idx) {
  rydb_config_index_t  *cf = idx->config;
  
  assert(cf->type == RYDB_INDEX_HASHTABLE);
  if(!rydb_file_open_index(db, idx)) {
    return 0;
  }
  if(!rydb_file_ensure_size(db, &idx->index, RYDB_INDEX_HASHTABLE_START_OFFSET)) {
    return 0;
  }
  idx->index.data.start = idx->index.file.start + RYDB_INDEX_HASHTABLE_START_OFFSET;
  rydb_hashtable_header_t *header = hashtable_header(idx);
  
  if(!header->active) {
    //write out header
    header->active = 1;
  }
  
  if(!cf->type_config.hashtable.direct_mapping) {
    if(!rydb_file_open_index_map(db, idx)) {
      rydb_file_close_index(db, idx);
      return 0;
    }
  }
  
  return 1;
}

#define HASHTABLE_VARS(db, idx, header, cf, entry_sz) \
  rydb_hashtable_header_t *header = hashtable_header(idx); \
  rydb_config_index_t *cf = idx->config; \
  size_t    entry_sz = hashtable_entry_size(cf);


//assumes val is at least as long as the indexed string
int rydb_index_hashtable_find_row(rydb_t *db, rydb_index_t *idx, char *val, rydb_row_t *row) {
  //printf("find row with val \"%s\"\n", val);
  HASHTABLE_VARS(db, idx, header, cf, bucket_sz);
  uint64_t  hashvalue, trimmed_hashvalue;
  rydb_stored_row_t *datarow;
  hashvalue = hash_value(db, cf, val, 0);
  rydb_rownum_t *bucket_rownum, rownum;
  rydb_rownum_t *buckets_end = (void *)&idx->index.data.start[bucket_sz * header->bucket.count.total];
  //rydb_hashtable_print(db, idx);
  
  const rydb_hashtable_bitlevel_count_t *bitlevel = NULL;
  while((bitlevel = hashtable_bitlevel_next(header, bitlevel)) != NULL) {
    trimmed_hashvalue = btrim64(hashvalue, 64 - bitlevel->bits);
    bucket_rownum = (rydb_rownum_t *)(idx->index.data.start + bucket_sz * trimmed_hashvalue);
    while((rownum = bucket_rownum < buckets_end ? *bucket_rownum : 0) != 0) {
      datarow = rydb_rownum_to_row(db, rownum);
      //printf("hash: %"PRIu64", bits: %"PRIu8" trimmed: %"PRIu64" rownum: %"PRIu32"%s", hashvalue, bitlevel->bits, trimmed_hashvalue, rownum, rownum ? "" : "\n");
      //printf("val: %s found: %s\n", val, &datarow->data[cf->start]);
      if(datarow && memcmp(val, &datarow->data[cf->start], cf->len) == 0) {
        if(bitlevel != &header->bucket.bitlevel.top) {
          //TODO: migrate this bucket to the top layer
        }
        rydb_storedrow_to_row(db, datarow, row);
        return 1;
      }
      bucket_rownum = (void *)((char *)bucket_rownum + bucket_sz);
    }
  }
  
  return 0;
}

int rydb_index_hashtable_add_row(rydb_t *db, rydb_index_t *idx, rydb_stored_row_t *row) {
  HASHTABLE_VARS(db, idx, header, cf, entry_sz);
  if(header->bucket.count.used+1 > header->bucket.count.load_factor_max) {
    if(!hashtable_grow(db, idx)) {
      return 0;
    }
    header = hashtable_header(idx); //file might have gotten remapped, get the header again
  }
  uint64_t  hashvalue = hash_value(db, cf, row->data, 64 - header->bucket.bitlevel.top.bits);
  rydb_rownum_t rownum = rydb_row_to_rownum(db, row);
  //printf("added rownum %"PRIu32" bits: %"PRIu8 " hashvalue %"PRIu64" trimmed to %"PRIu64" str: \"%s\"\n", rownum, header->bucket.bitlevel.top.bits, hash_value(db, cf, row->data, 0), hashvalue, row->data);
  char     *bucket = &idx->index.data.start[entry_sz * hashvalue];
  
  rydb_rownum_t *bucket_rownum = (void *)bucket;
  rydb_rownum_t *buckets_end = (void *)&idx->index.data.start[entry_sz * header->bucket.count.total];
  int buckets_skipped = 0;
  while(*bucket_rownum != 0) { //bucket is not empty
    //linear probing -- walk forward until we hit an empty bucket
    bucket_rownum = (void *)((char *)bucket_rownum + entry_sz);
    if(bucket_rownum >= buckets_end) {
      // don't loop around to the beginning -- just append this bucket as "overflow" to the end
      // we do this so that growing the hashtable is an in-place operation.
      // if the hashtable looped back to the beginning, the looped-back run from the last bucket would
      // need to be moved.
      if(!rydb_file_ensure_writable_address(db, &idx->index, bucket_rownum, entry_sz)) {
        return 0;
      }
      header = hashtable_header(idx); //file might have gotten remapped, get the header again
      assert(*bucket_rownum == 0); //appended data should be zerofilled
    }
    buckets_skipped ++;
  }
  hashtable_reserve(header);
  if(bucket_rownum >= buckets_end) {
    header->bucket.count.total++; //record bucket overflow
  }
  if(cf->type_config.hashtable.store_value) {
    memcpy(&bucket_rownum[1], &row->data[cf->start], cf->len);
  }
  *bucket_rownum = rownum;
  //rydb_hashtable_print(db, idx);
  header->bucket.count.used ++;
  header->bucket.bitlevel.top.count++;
  hashtable_release(header);
  return 1;
}
int rydb_index_hashtable_remove_row(rydb_t *db, rydb_index_t *idx, rydb_stored_row_t *row) {
  (void)(db);
  (void)(idx);
  (void)(row);
  return 1;
}
int rydb_index_hashtable_update_add_row(rydb_t *db,  rydb_index_t *idx, rydb_stored_row_t *row, off_t start, off_t end) {
  (void)(db);
  (void)(idx);
  (void)(row);
  (void)(start);
  (void)(end);
  return 1; 
}
int rydb_index_hashtable_update_remove_row(rydb_t *db,  rydb_index_t *idx, rydb_stored_row_t *row, off_t start, off_t end) {
  (void)(db);
  (void)(idx);
  (void)(row);
  (void)(start);
  (void)(end);
  return 1;
}

void rydb_hashtable_print(rydb_t *db, rydb_index_t *idx) {
  (void)(db);
  rydb_hashtable_header_t *header = hashtable_header(idx);
  char hash_key_hexstr_buf[33];
  for (unsigned i = 0; i < sizeof(db->config.hash_key.value); i ++) {
    sprintf(&hash_key_hexstr_buf[i*2], "%02x", db->config.hash_key.value[i]);
  }
  hash_key_hexstr_buf[sizeof(db->config.hash_key.value)*2]='\00';
  const char *fmt = "\nhashtable %s\n"
    "  key: %s\n"
    "  reserved:             %"PRIi8"\n"
    "  active:               %"PRIu8"\n"
    "  bucket count total:   %"PRIu32"\n"
    "  bucket count used:    %"PRIu32"\n"
    "  bucket count LF max:  %"PRIu32"\n"
    "  bucket size:          %"PRIu32"\n"
    "  bitlevels: top: bits:%2"PRIu8" n: %"PRIu32"\n";
    
  size_t    entry_sz = hashtable_entry_size(idx->config);
  
  printf(fmt, idx->config->name, hash_key_hexstr_buf, 
         header->reserved,
         header->active,
         header->bucket.count.total,
         header->bucket.count.used,
         header->bucket.count.load_factor_max,
         (uint32_t) entry_sz,
         header->bucket.bitlevel.top.bits,  header->bucket.bitlevel.top.count);
  for(int i=0; i<header->bucket.count.sub_bitlevels; i++) {
    printf("           %4d: bits: %2"PRIu8" n: %"PRIu32"\n", i+1, header->bucket.bitlevel.sub[i].bits, header->bucket.bitlevel.sub[i].count);
  }
  
  rydb_rownum_t *buckets_end = (void *)&idx->index.data.start[entry_sz * header->bucket.count.total];
  int i = 0;
  if(idx->index.data.start == (char *)buckets_end) {
    printf("  <EMPTY>\n");
  }
  else {
    for(rydb_rownum_t *bucket = (void *)idx->index.data.start; bucket < buckets_end; bucket = (void *)((char *)bucket + entry_sz)) {
      if(*bucket) {
        printf("  %p [%3"PRIu32"] <%5"PRIu32">\n", (void *)bucket, i, *bucket);
      }
      else {
        printf("  %p [%3"PRIu32"] <EMPTY>\n", (void *)bucket, i);
      }
      i++;
    }
  }
}
