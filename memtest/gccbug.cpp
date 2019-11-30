#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

void print(void *cpy) {
  printf("%u\n", reinterpret_cast<uint16_t*>(cpy)[0]);
}

int main() {
  void *cpy = calloc(1, 4);
  uint32_t *ptr = reinterpret_cast<uint32_t*>(cpy);
  for (int i = 0; i < 1; i++) {
    ptr[i] = 1;
  }
  print(cpy);
}
