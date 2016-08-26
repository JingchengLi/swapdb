#include "gtest/gtest.h"
#include "nemo_test.h"

int main(int argc, char *argv[])
{
	FILE *LOG_FILE = fopen(LOG_FILE_NAME, "w");
	fclose(LOG_FILE);
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
