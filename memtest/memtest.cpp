
#include <iostream>
#include <stdlib.h>
#include <memory.h>
#include <stdint.h>
#include <algorithm>
#include <chrono>


using namespace std;



// Because I'm lazy and I used a C++11 syntax to initialise a vector, you have to use G++ to compile this,
// Compile with something like g++ -pedantic -std=c++0x RadixSort.cpp
// Or for the more pedantic g++ -pedantic -std=c++0x -Wall -Wextra -Werror=return-type -Wno-reorder RadixSort.cpp
// Reference http://en.wikipedia.org/wiki/Radix_sort
#include <iostream>
#include <vector>
#include <algorithm>        // Required
#include <iterator>            // Required
#include <queue>            // Required
using namespace std;

// Radix Sort using base-10 radix
template <typename InputIterator, typename OutputIterator> void radixSort(InputIterator start, InputIterator end, OutputIterator output){
    const int BASE = 65536;    // Base
    std::queue<  typename std::iterator_traits<OutputIterator>::value_type > bucket[BASE];    // An array of buckets based on the base

    unsigned size = end - start;
    // Get the maximum number
    typename std::iterator_traits<InputIterator>::value_type max = 0xFFFFFFFF;// *std::max_element(start, end);    // O(n)

    // Copy from input to output
    std::copy(start, end, output);    // O(n)

    // Iterative Radix Sort - if k is log BASE (max), then the complexity is O( k * n)
    for (unsigned power = 1; max != 0; max /= BASE, power*=BASE){ // If BASE was 2, can use shift. Although compiler should be smart enough to optimise this.

        // Assign to buckets
        for (OutputIterator it = output; it != output+size; it++){
            unsigned bucketNumber =  (unsigned) ( (*it/power)  % BASE );
            bucket[bucketNumber].push(*it);        // O(1)
        }

        // Re-assemble
        OutputIterator it = output;
        for (int i = 0; i < BASE; i++){
            int bucketSize = bucket[i].size();
            for (int j = 0; j < bucketSize; j++){
                *it = bucket[i].front();
                bucket[i].pop();
                it++;
            }
        }
    }
}

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)


typedef uint8_t mem_type_t;

#define MEMORY_SIZE_EXP ((30 + 0) - __builtin_ctz(sizeof(mem_type_t)))
#define MEMORY_SIZE (1ULL << MEMORY_SIZE_EXP)
#define LINE_SIZE  (1024ULL * 16ULL)

#define ITERATIONS_NUMBER (640000000ULL*8)

#define BSIZE 200000
#define BSIZE2 4096
#define BEXP 8


#define BUCKET_EXP (MEMORY_SIZE_EXP - BEXP*2)
#define BUCKET_SIZE (1 << BUCKET_EXP)


#define BNUM (1 << BEXP)
uint64_t (&buffer)[BNUM][BSIZE] = *(uint64_t (*)[BNUM][BSIZE])calloc(1, sizeof(buffer));
uint64_t (&bufferv2)[BNUM][BNUM][BSIZE2] = *(uint64_t (*)[BNUM][BNUM][BSIZE2])calloc(1, sizeof(bufferv2));
uint32_t counters[BNUM];
uint16_t countersv2[BNUM][BNUM];

uint64_t vals[256] = {2, 423423444577, 29831273812, 892173, 0};

__uint128_t g_lehmer64_state = 340463374607431768ULL;
uint64_t lehmer64() {
  g_lehmer64_state *= 0xda942042e4dd58b5;
  return g_lehmer64_state >> 64;
}

// //making random accesses to memory:
// unsigned int next(unsigned int current){
//    return (current*10001+328)%SIZE;
// }
//
// //the actual work is happening here
// void do_work(){
//
//     //set up the oracle - let see it in the future oracle_offset steps
//     unsigned int prefetch_index=0;
//     for(int i=0;i<oracle_offset;i++)
//         prefetch_index=next(prefetch_index);
//
//     unsigned int index=0;
//     for(int i=0;i<STEP_CNT;i++){
//         //use oracle and prefetch memory block used in a future iteration
//         if(prefetch){
//             __builtin_prefetch(mem.data()+prefetch_index,0,1);
//         }
//
//         //actual work, the less the better
//         result+=mem[index];
//
//         //prepare next iteration
//         prefetch_index=next(prefetch_index);  #update oracle
//         index=next(mem[index]);               #dependency on `mem[index]` is VERY important to prevent hardware from predicting future
//     }
// }

int main() {
  mem_type_t *mem = (mem_type_t*)calloc(1, MEMORY_SIZE * sizeof(mem_type_t));
  auto start = chrono::high_resolution_clock::now();
  uint32_t hash = 0;
  double test = 0.0;
  for (uint64_t i = 0; i < ITERATIONS_NUMBER; i++) {
    uint64_t val = lehmer64();
    // uint8_t cnt = val % 2 != 0;
    // mem[val % MEMORY_SIZE] = 1;
    // continue;
    // mem_type_t *ptrs[2] = { &val, &mem[i % MEMORY_SIZE] };
    // __builtin_prefetch(ptrs[cnt]);

    // for (int j = 0; j < 1; j++) {
    //   test = test * (j ^ i);
    // }

    // *ptrs[cnt] = val;

    uint32_t b1idx = val % BNUM;

    if (counters[b1idx] == BSIZE) {
      for (int j = 0; j < BSIZE; j++) {
        uint32_t b2idx = buffer[b1idx][j] % BNUM;
        if (countersv2[b1idx][b2idx] == BSIZE2) {
          for (int k = 0; k < BSIZE2; k++) {
            uint64_t addr = b1idx * (1ULL << (BEXP + BUCKET_EXP)) + b2idx * (1 << BUCKET_EXP) + bufferv2[b1idx][b2idx][k] % BUCKET_SIZE;
            // cout << hex << "0x" << addr << endl;
            mem_type_t &val = mem[addr];

            struct tmp {
              union {
                mem_type_t v1;
                uint8_t v2[1];
              };
            } tmp;
            tmp.v1 = val;

            tmp.v1++;
            // tmp.v2[2]+=tmp.v2[1];
            // tmp.v2[3]++;

            val = tmp.v1;

          }
          countersv2[b1idx][b2idx] = 0;
        }
        else {
          bufferv2[b1idx][b2idx][countersv2[b1idx][b2idx]++] = buffer[b1idx][j] / BNUM;
        }
      }
      counters[b1idx] = 0;
    }
    else {
      buffer[b1idx][counters[b1idx]++] = val / BNUM;
    }
    // hash ^= vals[i%256];
  }

  auto end = chrono::high_resolution_clock::now();
  cout << test << endl;

  // uint32_t hash = 0;
  // uint32_t counter = 0;
  // for (uint64_t i = 0; i < ITERATIONS_NUMBER; i++) {
  //   // uint32_t bucket = (counter/(8192) * 1000000007) % (MEMORY_SIZE / BUCKET_SIZE) * BUCKET_SIZE;
  //   // mem_type_t *val = &mem[(bucket + (hash % BUCKET_SIZE)) % MEMORY_SIZE];//++;//rand() % MEMORY_SIZE;//;
  //   // val += 1;
  //   // struct aaa {
  //   //   union {
  //   //     mem_type_t v1;
  //   //     uint16_t v2[sizeof(mem_type_t)/2];
  //   //   };
  //   // } test = *(aaa*)val;
  //   // for (int k = 0; k < sizeof(mem_type_t)/2; k++) {
  //   //   if (test.v2[k] == 0xFF) {
  //   //     hash++;
  //   //     test.v2[k] = 44;
  //   //     break;
  //   //   }
  //   // }
  //   hash = (hash + 377) * 10000000007;
  //   counter += 1;
  //   // *val = test.v1;
  //   // if () {
  //     // uint32_t bucket = hash % BNUM;
  //     // buffer[bucket][counters[bucket]++ % 1000000] = hash;
  //     buffer[0][0] = (hash * (hash % 12 == 4));
  //   // }
  //
  // }


  uint64_t sum = 0;
  uint64_t tot = 0;
  for (uint64_t i = 0; i < MEMORY_SIZE; i++) {
    sum += mem[i] % 256;
    tot += mem[i] != 0;
  }
  printf("Tot: %.2fM, Sum: %lu\n", tot / 1024.0f / 1024.0f, sum);// = i;

  // floating-point duration: no duration_cast needed
  std::chrono::duration<double> fp_ms = end - start;

  float mops = ITERATIONS_NUMBER / fp_ms.count() / 1000000;

  printf("Elapsed: %.2f rate: %.2fM/s %.2fcycles/op \n", fp_ms.count(), mops, 4100.0f / mops );

  // for (uint64_t i = 0; i < MEMORY_SIZE / 64; i++) {
  //   mem[i % 3] = i % 0xFF;
  // }


  // uint32_t *mem_out = (uint32_t*)malloc(MEMORY_SIZE * sizeof(uint32_t));
  // volatile char *cache = (volatile char*)malloc(LINE_SIZE * BULK_SIZE);

  // for (uint64_t i = 0; i < MEMORY_SIZE; i++) {
  //   mem[i] = rand();
  // }

  // vector<unsigned> input = {5,10,15,20,25,50,40,30,20,10,9524,878,17,1,99,18785,3649,164,94,
  //     123,432,654,3123,654,2123,543,131,653,123,533,1141,532,213,2241,824,1124,42,134,411,
  //     491,341,1234,527,388,245,1992,654,243,987};
  // vector<unsigned> output(input.size());

  // radixSort(mem, mem + MEMORY_SIZE, mem_out);
  // sort(mem, mem + MEMORY_SIZE);
  // cout << memcmp(mem, mem_out, MEMORY_SIZE * sizeof(uint32_t)) << endl;

  // C++11 ranged based for loops
  // http://www.cprogramming.com/c++11/c++11-ranged-for-loop.html
  // for (unsigned it : output){
  //     cout << it << endl;
  // }
  //



  // for (uint64_t i = 0; i < ITERATIONS_NUMBER / BULK_SIZE; i++) {
  //
  //   uint64_t page = (rand() ^ (((uint64_t)rand()) << 32ULL)) % (MEMORY_SIZE / LINE_SIZE);
  //
  //   for (int j = 0; j < BULK_SIZE; j++) {
  //     mem[page * LINE_SIZE + rand() % LINE_SIZE] += 1;
  //   }
  // }

}
