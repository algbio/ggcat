//
// Created by andrea on 20/04/21.
//


#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <sstream>
#include <atomic>

using namespace std;

int rindex;
double results[17][17];

atomic<int> ready_cnt;
int ready_val = 0;

void testing(int index, int size) {
    size_t SIZE = 1024ULL * 1024 * 1024 * size / 4;
    std::default_random_engine generator;


    const uint32_t accessed_mem = 1024 * 8;
    const uint32_t data_divider = 16;

    std::uniform_int_distribution<uint32_t> distribution(0, accessed_mem);
    std::uniform_int_distribution<uint32_t> bucket_distrib(0, SIZE);

    uint32_t *data = new uint32_t[SIZE];

    uint32_t area = 0;

    for (size_t i = 0; i < SIZE; i++) {

        if (i % (accessed_mem / data_divider) == 0) {
            area = bucket_distrib(generator) / accessed_mem * accessed_mem;
        }

        data[i] = area + distribution(generator); // (int)(i * 377ULL) & 0xFFFFF;//
    }

    std::cout << "LOADING..." << std::endl;

    ready_cnt.fetch_add(1);
    while (ready_cnt.load() != ready_val) continue;

    size_t sum = 0;

//    const int BUCKETS = 1024;
//
//    uint32_t positions[BUCKETS] = {0};
//    for (int i = 0; i < BUCKETS; i++) {
//        positions[i] = SIZE * i / BUCKETS;
//    }

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    asm volatile("" ::: "memory");
    for (size_t i = 0; i < SIZE; i++) {
//        sum += data[positions[data[i] % BUCKETS]++];
        sum += data[data[i]];
    }
    asm volatile("" ::: "memory");
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    asm volatile("" ::: "memory");

    size_t nano_seconds = std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count();

    std::cout << "SUM = " << sum << std::endl;
    double speed = (((double)SIZE * 4 * 1000000000 / (double)nano_seconds / (1024 * 1024 * 1024)));
    std::cout << "Write speed = " << speed << "GB/s [" << index << "]" << std::endl;
//    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
//    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;
//    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;
//    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "[ns]" << std::endl;

    results[rindex][index] = speed;

    delete[] data;
}

int main() {

    for (int threads = 1; threads <= 16; threads++) {

        ready_cnt.store(0);
        ready_val = threads;

        rindex = threads - 1;
        vector<std::thread> vec;
        vec.reserve(threads);
        size_t memory = 16;
        for (int i = 0; i < threads; i++) {
            vec.emplace_back(testing, i, 4); // memory / threads
        }
        for (int i = 0; i < threads; i++) {
            vec[i].join();
        }
    }

    cout << "Results: " << endl;
    for (int i = 0; i < 16; i++) {
        stringstream ss;
        for (int j = 0; j <= i; j++) {
            ss << results[i][j] << ";";
        }
        cout << ss.str() << endl;
    }
}
