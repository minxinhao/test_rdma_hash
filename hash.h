//
// Created by YiwenZhang on 2022/4/2.
//

#ifndef RACE_HASH_H
#define RACE_HASH_H

#include <stdint.h>
#include <stddef.h>

size_t standard(const void* _ptr, size_t _len,size_t _seed=static_cast<size_t>(0xc70f6907UL));
size_t jenkins(const void* _ptr, size_t _len, size_t _seed=0xc70f6907UL);
size_t murmur2 ( const void * key, size_t len, size_t seed=0xc70f6907UL);
uint64_t xxhash(const void *data, size_t length, size_t seed);

size_t hash_1(const void* _ptr, size_t _len);
size_t hash_2(const void* _ptr, size_t _len);

#endif //RACE_HASH_H