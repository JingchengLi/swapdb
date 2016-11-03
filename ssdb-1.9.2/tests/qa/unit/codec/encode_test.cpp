#include "gtest/gtest.h"
#include "encode.h"
#include "ssdb/const.h"

class EncodeTest : public testing::Test
{
    void SetUp(){}
	void TearDown(){}
};

TEST(EncodeTest, Test_encode_meta_key) {
    string key="0";
  string meta_key = encode_meta_key("key");

  EXPECT_EQ(meta_key , "11");
}
