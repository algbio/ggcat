
#include <iostream>
#include <cstdlib>
#include <memory.h>
#include <cstdint>
#include <chrono>
#include <cassert>
#include <thread>

using namespace std;

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

typedef uint16_t mem_type_t;

#define MEMORY_SIZE_EXP ((30 + 1) - __builtin_ctz(sizeof(mem_type_t)))
#define MEMORY_SIZE (1ULL << MEMORY_SIZE_EXP)
#define LINE_SIZE  (1024ULL * 16ULL)

#define ITERATIONS_NUMBER (6400000000ULL*2)

#define BSIZE (65536*8)
#define BSIZE2 (2048*3)
#define BEXP 8


#define BUCKET_EXP (MEMORY_SIZE_EXP - BEXP*2)
#define BUCKET_SIZE (1 << BUCKET_EXP)


#define BNUM (1 << BEXP)


#define RESERVE_SIZE (1024 * 1024 * 16)

inline uint64_t lehmer64(__uint128_t &g_lehmer64_state) {
    g_lehmer64_state *= 0xda942042e4dd58b5;
    return g_lehmer64_state >> 64;
}

int forked() {

    uint32_t (&buffer)[BNUM][BSIZE] = *(uint32_t (*)[BNUM][BSIZE])calloc(1, sizeof(buffer));
    uint32_t (&bufferv2)[BNUM][BNUM][BSIZE2] = *(uint32_t (*)[BNUM][BNUM][BSIZE2])calloc(1, sizeof(bufferv2));
    uint32_t counters[BNUM];
    uint16_t countersv2[BNUM][BNUM];

    uint64_t (&reserve)[RESERVE_SIZE] = *(uint64_t (*)[RESERVE_SIZE])calloc(1, sizeof(reserve));
    uint32_t reserve_idx;

    __uint128_t g_lehmer64_state = 340463374607431768ULL;



    auto *mem = (mem_type_t *) calloc(1, MEMORY_SIZE * sizeof(mem_type_t));
    auto start = chrono::high_resolution_clock::now();
    uint32_t hash = 0;
    double test = 0.0;
    char tbuffer[8192];
    char sbuffer[8192];
    size_t read;
    size_t rsize;

//    memset(mem, -1, MEMORY_SIZE * sizeof(mem_type_t));
//    memset(mem, 7, MEMORY_SIZE * sizeof(mem_type_t));
//    memset(mem, 1, MEMORY_SIZE * sizeof(mem_type_t));
    for (uint64_t i = 0; i < ITERATIONS_NUMBER;) {
        rsize = 10000000;
        //    // Read quality
        //    do {
        //      read = 8192;
        //      char *buf = tbuffer;
        //      read = getline(&buf, &read, stdin);
        //    } while (tbuffer[0] != '@');
        //    for(;;) {
        //      read = 8192;
        //      char *buf = tbuffer;
        //      read = getline(&buf, &read, stdin);
        //      if (tbuffer[read-1] == '\n') read--;
        //      if (tbuffer[0] == '+') break;
        //      memcpy(sbuffer + rsize, tbuffer, read);
        //      rsize += read;
        //    }

        for (int j = 0; j < rsize && i < ITERATIONS_NUMBER; i++, j++) {
            uint64_t val = lehmer64(g_lehmer64_state);
            // mem_type_t *ptrs[2] = { &val, &mem[i % MEMORY_SIZE] };
            // __builtin_prefetch(ptrs[cnt]);

            uint32_t b1idx = val % BNUM;
            if (counters[b1idx] == BSIZE) {
                for (int k = 0; k < BNUM; k++) {
                    uint32_t b1idx = k;
                    uint32_t size = counters[b1idx];
//                    assert(size <= BSIZE);
                    for (int j = 0; j < size; j++) {
                        uint32_t b2idx = buffer[b1idx][j] % BNUM;
//                        ended |= (j ^ size) != 0;
                        if (countersv2[b1idx][b2idx] == BSIZE2) {
//                            cout << "AAA" << endl;
                            for (int w = 0; w < BSIZE2; w++) {
                                uint64_t addr = (b1idx << (BEXP + BUCKET_EXP)) +
                                                (b2idx << BUCKET_EXP) +
                                                bufferv2[b1idx][b2idx][w] % BUCKET_SIZE;
//                                 cout << hex << "0x" << addr << endl;
                                mem_type_t &val = mem[addr];

                                val++;
                                reserve[reserve_idx++ % RESERVE_SIZE] = addr;
//                                struct tmp {
//                                    union {
//                                        mem_type_t v1;
//                                        uint8_t v2[1];
//                                    };
//                                } tmp;
//                                tmp.v1 = val;
//
//                                tmp.v1++;
                                // tmp.v2[2]+=tmp.v2[1];
                                // tmp.v2[3]++;

//                                val = tmp.v1;

                            }
                            countersv2[b1idx][b2idx] = 0;
                        } else {
                            bufferv2[b1idx][b2idx][countersv2[b1idx][b2idx]++] =
                                    buffer[b1idx][j] / BNUM;
                        }
                    }

                    counters[b1idx] = 0;
                }
            } else {
                buffer[b1idx][counters[b1idx]++] = val / BNUM;
            }
        }
    }
    auto end = chrono::high_resolution_clock::now();

    uint64_t sum = 0;
    uint64_t tot = 0;
    for (uint64_t i = 0; i < MEMORY_SIZE; i++) {
        sum += mem[i] % 256;
        tot += mem[i] != 0;
    }
    cout << reserve_idx << endl;
    printf("Tot: %.2fM, Sum: %lu\n", tot / 1024.0f / 1024.0f, sum);// = i;

    // floating-point duration: no duration_cast needed
    std::chrono::duration<double> fp_ms = end - start;

    float mops = ITERATIONS_NUMBER / fp_ms.count() / 1000000;

    printf("Elapsed: %.2f rate: %.2fM/s %.2fcycles/op \n", fp_ms.count(), mops, 4100.0f / mops);
}


int main() {
    const int NUMTHREADS = 6;

    thread threads[NUMTHREADS];

    for (int i = 0; i < NUMTHREADS; i++) {
        threads[i] = thread(forked);
    }

    for (int i = 0; i < NUMTHREADS; i++) {
        threads[i].join();
    }
}