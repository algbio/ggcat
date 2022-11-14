
.PHONY: all ggcat-capi ggcat-source

all: ggcat-capi

ggcat-capi: lib/libggcat_cpp_bindings.a
	g++ -std=c++11 -I./include -I./include -L./lib src/ggcat.cc -lggcat_cpp_bindings -lggcat_cxx_interop -o ggcat-capi

lib/libggcat_cpp_bindings.a: ggcat-source
	cargo build --release --package ggcat-cpp-bindings
	cp ../../../target/release/libggcat_cpp_bindings.a lib/
