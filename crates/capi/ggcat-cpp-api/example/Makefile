
.PHONY: all ggcat-capi ggcat-source

all: ggcat-example

ggcat-example: ../lib/libggcat_cpp_bindings.a ../lib/libggcat_cxx_interop.a ../lib/libggcat_api.a
	g++ -flto -std=c++11 -O3 -I../include -L../lib main.cc -lggcat_api -lggcat_cpp_bindings -lggcat_cxx_interop -o ggcat-example -lpthread -ldl
