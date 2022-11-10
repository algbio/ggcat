
#include <iostream>
#include "ggcat-api.h"

using namespace std;
using namespace rust::cxxbridge1;

int main()
{
	cout << "Print from C++!" << endl;
	print_test(Slice<const unsigned char>((const unsigned char *)"Hello!", 6));
	return 0;
}
