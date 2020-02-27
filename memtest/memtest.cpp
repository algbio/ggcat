
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

typedef __uint128_t mem_type_t;

#define MEMORY_SIZE_EXP ((30+1) - __builtin_ctz(sizeof(mem_type_t)))
#define MEMORY_SIZE (1ULL << MEMORY_SIZE_EXP)
#define LINE_SIZE  (1024ULL * 16ULL)

#define ITERATIONS_NUMBER (1024ULL * 1024 * 1024 * 384 / 100)

#define BSIZE (65536U*4U)
#define BSIZE2 (2048U*2)
#define BEXP 8U
#define BEXP2 8U


#define BUCKET_EXP (MEMORY_SIZE_EXP - (BEXP + BEXP2))
#define BUCKET_SIZE (1 << BUCKET_EXP)


#define BNUM (1 << BEXP)
#define BNUM2 (1 << BEXP2)


#define RESERVE_SIZE (1024 * 1024 * 16)

#define BATCH_SIZE (65536)
#define BATCH_SIZE2 (768)

inline uint64_t lehmer64(__uint128_t &g_lehmer64_state) {
    g_lehmer64_state *= 0xda942042e4dd58b5;
    return g_lehmer64_state >> 64;
}


//inline void write_bloom(mem_type_t *bloom, uint64_t address, uint64_t entropy) {
//
//}

#define MASK_FROM_BOOLEAN_PASS_IF_TRUE(b) (-(mem_type_t)(b))
#define MASK_FROM_BOOLEAN_PASS_IF_FALSE(b) (((((mem_type_t)-1) + (mem_type_t)(b))))


int forked() {

    uint64_t (&buffer)[BNUM][BSIZE] = *(uint64_t (*)[BNUM][BSIZE])calloc(1, sizeof(buffer));
    uint32_t (&bufferv2)[BNUM][BNUM2][BSIZE2] = *(uint32_t (*)[BNUM][BNUM2][BSIZE2])calloc(1, sizeof(bufferv2));
    uint32_t counters[BNUM] = {0};
    uint16_t countersv2[BNUM][BNUM2] = {0};

    uint64_t (&reserve)[RESERVE_SIZE] = *(uint64_t (*)[RESERVE_SIZE])calloc(1, sizeof(reserve));
    uint64_t reserve_idx = 0;
    uint64_t duplicates_cnt = 0;
    /// TODO: Second/third level counting sort to separate l1 caches (16kb max) + reduce to 6 the BNUM/BNUM2 + 42/50 bit l1 bits count

    __uint128_t g_lehmer64_state = 340463374607431768ULL;



    auto *mem = (mem_type_t *) calloc(1, MEMORY_SIZE * sizeof(mem_type_t));
    auto start = chrono::high_resolution_clock::now();
    uint32_t hash = 0;
    double test = 0.0;
    char tbuffer

    ;
    char sbuffer

    ;
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
            for (int b = 0; b < BATCH_SIZE && i < ITERATIONS_NUMBER; b++, i++) {
                uint64_t val = lehmer64(g_lehmer64_state);
                uint32_t b1idx = val % BNUM;
                buffer[b1idx][counters[b1idx]++] = val / BNUM;
            }
            // mem_type_t *ptrs[2] = { &val, &mem[i % MEMORY_SIZE] };
            // __builtin_prefetch(ptrs[cnt]);

            for (int c = 0; c < BNUM; c++) {
                if (counters[c] >= BSIZE - BATCH_SIZE) {
//                    uint32_t size = counters[c];
//                    for (int j = 0; j < size; j++) {
//                        for (int b = 0; b < BATCH_SIZE2 && j < size; b++, j++) {
//                            uint32_t b2idx = buffer[c][j] % BNUM2;
//                            bufferv2[c][b2idx][countersv2[c][b2idx]++] =
//                                    buffer[c][j] / BNUM2;
//                        }
//                        for (int c2 = 0; /*c2 < BNUM2*/false; c2++) {
//                            if (countersv2[c][c2] > BSIZE2 - BATCH_SIZE2) {
//                                uint32_t size2 = countersv2[c][c2];
//                                uint64_t base_address = (((uint64_t)c) << (BEXP2 + BUCKET_EXP)) +
//                                                        (((uint64_t)c2) << BUCKET_EXP);
//                                for (int w = 0; w < size2; w++) {
//                                    uint64_t addr = base_address +
//                                                    bufferv2[c][c2][w] % BUCKET_SIZE;
//                                    mem_type_t &val = mem[addr];
//
//                                    mem_type_t is_present_mask;
//                                    uint8_t count = 0;
//
//                                    mem_type_t mask = 0;
//                                    mask |= ((mem_type_t)1) << ((bufferv2[c][c2][w] >>  BUCKET_EXP)       & 0x7fU);
//                                    is_present_mask = MASK_FROM_BOOLEAN_PASS_IF_TRUE((val & mask) == mask);
//                                    count += (val & mask) == mask;
//
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 3))  & 0x7fU));
//                                    is_present_mask = MASK_FROM_BOOLEAN_PASS_IF_TRUE((val & mask) == mask);
//                                    count += (val & mask) == mask;
//
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 6))  & 0x7fU));
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 9))  & 0x7fU));
//                                    is_present_mask = MASK_FROM_BOOLEAN_PASS_IF_TRUE((val & mask) == mask);
//                                    count += (val & mask) == mask;
//
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 12)) & 0x7fU));
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 15)) & 0x7fU));
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 18)) & 0x7fU));
//                                    is_present_mask = MASK_FROM_BOOLEAN_PASS_IF_TRUE((val & mask) == mask);
//                                    count += (val & mask) == mask;
//
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 21)) & 0x7fU));
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 24)) & 0x7fU));
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 27)) & 0x7fU));
//                                    mask |= is_present_mask & (((mem_type_t)1) << ((bufferv2[c][c2][w] >> (BUCKET_EXP + 30)) & 0x7fU));
//                                    count += (val & mask) == mask;
//
//                                    reserve[reserve_idx % RESERVE_SIZE] = addr;
//                                    bool is_present = (val & mask) == mask;
//                                    bool fourth_occurrence = count == 4;
//                                    bool threshold_limit = (__builtin_popcountll((uint64_t )val) + (__builtin_popcountll((uint64_t)(val >> 64U)))) >= 64;
//                                    val |= mask & MASK_FROM_BOOLEAN_PASS_IF_FALSE(threshold_limit);
//                                    reserve_idx += !is_present && threshold_limit;
//                                    duplicates_cnt += fourth_occurrence;
//                                }
//                                countersv2[c][c2] = 0;
//                            }
//                        }
//                    }
                    counters[c] = 0;
                }
            }
        }
    }
    auto end = chrono::high_resolution_clock::now();

    uint64_t sum = 0;
    uint64_t tot = 0;
    for (uint64_t i = 0; i < MEMORY_SIZE; i++) {
        sum += __builtin_popcountll((uint64_t)mem[i]);
        sum += __builtin_popcountll((uint64_t)(mem[i] >> 64U));
        tot += mem[i] != 0;
    }
    cout << reserve_idx << endl;
    printf("Tot: %.2fM, Sum: %llu [%.2f%%] dupl: %llu\n", tot / 1024.0f / 1024.0f, sum, 100 * sum / (float)(MEMORY_SIZE * sizeof(mem_type_t) * 8), duplicates_cnt);// = i;

    // floating-point duration: no duration_cast needed
    std::chrono::duration<double> fp_ms = end - start;

    float mops = ITERATIONS_NUMBER / fp_ms.count() / 1000000;

    printf("Elapsed: %.2f rate: %.2fM/s %.2fcycles/op \n", fp_ms.count(), mops, 4100.0f / mops);
}


int main() {
    const int NUMTHREADS = 5;

    thread threads[NUMTHREADS];

    for (int i = 0; i < NUMTHREADS; i++) {
        threads[i] = thread(forked);
    }

    for (int i = 0; i < NUMTHREADS; i++) {
        threads[i].join();
    }
}