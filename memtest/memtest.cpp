
#include <iostream>
#include <stdlib.h>
#include <memory.h>
#include <stdint.h>
#include <algorithm>


using namespace std;

#define MEMORY_SIZE (1024ULL * 1024ULL * 128ULL * 1ULL)
#define LINE_SIZE  (1024ULL * 16ULL)
#define BULK_SIZE (256)

#define ITERATIONS_NUMBER 8000000


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

int main() {
  uint32_t *mem = (uint32_t*)malloc(MEMORY_SIZE * sizeof(uint32_t));

  for (int i = 0; i < ITERATIONS_NUMBER; i++) {
    
  }


  uint32_t *mem_out = (uint32_t*)malloc(MEMORY_SIZE * sizeof(uint32_t));
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
