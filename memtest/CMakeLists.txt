cmake_minimum_required(VERSION 3.15)
project(memtest)

set(CMAKE_CXX_STANDARD 14)
add_executable(memtest
        memtest.cpp)

find_package (Threads)
target_link_libraries (memtest ${CMAKE_THREAD_LIBS_INIT})

