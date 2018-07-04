
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <algorithm>

#include "base/util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore;

TEST(STATUS, Basic) {
    int times = 100;

    int err = 0;
    while (times--) {
        std::string pre = randomString(randomInt() % 10 + 10);
        std::vector<std::string> keys;

        int len = randomInt() % 10000;
        if (len < 4) continue;

        for (int i=0; i<len; i++) {
            keys.push_back(pre+randomString(randomInt() % 500));
        }

        std::sort(keys.begin(), keys.end());

        std::string sk = keys[0];
        std::string ek = keys[len-1];

        std::string lk = keys[len/2-1];
        std::string rk = keys[len/2];
        std::string sp = SliceSeparate(lk, rk, sk.length() + 5);

        for (int i=0; i<len; i++) {
            if (sp < keys[i]) {
                int k = len / 2 - i;
                if (k > 10 || k < -10) {
                    ++err;
                }
                break;
            }
        }


        ASSERT_GE(sp, sk) << "\nsk:" << sk << "\nlk:" << lk << "\nrk:" << rk << "\nek:" << ek << std::endl;
        ASSERT_LE(sp, ek) << "\nsk:" << sk << "\nlk:" << lk << "\nrk:" << rk << "\nek:" << ek << std::endl;
    }

    std::cout << "offset > 10: " << err  << " ratio:"<< err * 1.0 / 1000 <<"%" << std::endl;
}

} /* namespace  */
