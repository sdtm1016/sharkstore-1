#include "util.h"

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

namespace sharkstore {

static thread_local unsigned seed = time(nullptr);

int randomInt() { return rand_r(&seed); }

std::string randomString(size_t length) {
    static const char chars[] = "abcdefghijklmnopqrstuvwxyz"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    std::string str;
    str.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        str.push_back(chars[randomInt() % (sizeof(chars) - 1)]);
    }
    return str;
}

std::string strErrno(int errno_copy) {
    static thread_local char errbuf[1025] = {'\0'};
#ifdef __linux__
    char *ret = ::strerror_r(errno_copy, errbuf, 1024);
    return std::string(ret);
#elif defined(__APPLE__)
    ::strerror_r(errno_copy, errbuf, 1024);
    return std::string(errbuf);
#else
#error unsupport platform
#endif
}

std::string EncodeToHex(const std::string& src) {
    std::string result;
    result.reserve(src.size() * 2);
    char buf[3];
    for (std::string::size_type i = 0; i < src.size(); ++i) {
        snprintf(buf, 3, "%02X", static_cast<unsigned char>(src[i]));
        result.append(buf, 2);
    }
    return result;
}

// most of the code is from rocksdb
static int fromHex(char c) {
    if (c >= 'a' && c <= 'f') {
        c -= ('a' - 'A');
    }
    if (c < '0' || (c > '9' && (c < 'A' || c > 'F'))) {
        return -1;
    }
    if (c <= '9') {
        return c - '0';
    }
    return c - 'A' + 10;
}

bool DecodeFromHex(const std::string& hex, std::string* result) {
    auto len = hex.size();
    if (len % 2 != 0 || result == nullptr) {
        return false;
    }

    result->clear();
    result->reserve(len/2);
    for (size_t i = 0; i < len; i += 2) {
        int h1 = fromHex(hex[i]);
        if (h1 < 0) {
            return false;
        }
        int h2 = fromHex(hex[i+1]);
        if (h2 < 0) {
            return false;
        }
        result->push_back(static_cast<char>((h1 << 4) | h2));
    }
    return true;
}

std::string SliceSeparate(const std::string &l, const std::string &r, size_t max_len) {
    if (l.empty() || r.empty()) {
        return std::string();
    }

    size_t l_len = l.length();
    size_t r_len = r.length();

    size_t len = l_len;

    if (l_len > r_len) {
        len = r_len;
    }

    int cr = 0;
    for (size_t i = 0; i < len; i++) {
        cr = l[i] - r[i];
        if (cr > 0) {
            return l.substr(0, i + 1);
        }
        if (cr < 0) {
            return r.substr(0, i + 1);
        }
        if (max_len > 0 && i > max_len) {
            return r.substr(0, i);
        }
    }

    if (l_len == r_len) {
        return l;
    }
    if (l_len > r_len) {
        return l.substr(0, len + 1);
    }

    return r.substr(0, len + 1);
}

std::string NextComparable(const std::string& str) {
    std::string result;
    if (!str.empty()) {
        result.reserve(str.size());
    }
    for (auto it = str.crbegin(); it != str.crend(); ++it) {
        auto ch = static_cast<uint8_t>(*it);
        if (ch < 0xff) {
            result.assign(str.cbegin(), it.base());
            result.back() = static_cast<char>(ch + 1);
            return result;
        }
    }
    return result;
}

#ifdef __linux__
static const char kPathSeparator = '/';
#elif defined(__APPLE__)
static const char kPathSeparator = '/';
#else
#error unsupported platform
#endif

std::string JoinFilePath(const std::vector<std::string> &strs) {
    std::string ret = strs.empty() ? "" : strs[0];
    for (size_t i = 1; i < strs.size(); ++i) {
        ret.push_back(kPathSeparator);
        ret += strs[i];
    }
    return ret;
}

int CheckDirExist(const std::string& path) {
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::stat(path.c_str(), &sb);
    if (ret != 0) {
        return ret;
    } else if (!S_ISDIR(sb.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    return 0;
}

int MakeDirAll(const std::string &path, mode_t mode) {
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::stat(path.c_str(), &sb);
    if (0 == ret) {
        if (!S_ISDIR(sb.st_mode)) {  // 路径存在，但是不是目录
            errno = ENOTDIR;
            return -1;
        } else {
            return 0;
        }
    } else if (errno != ENOENT) {
        return -1;
    }

    // 跳过末尾多余的路径分隔符
    size_t i = path.length();
    while (i > 0 && path[i - 1] == kPathSeparator) {
        --i;
    }

    // 从后往前扫描，找到最后一个路径的位置
    size_t j = i;
    while (j > 0 && path[j - 1] != kPathSeparator) {
        --j;
    }

    if (j > 1) {
        ret = MakeDirAll(path.substr(0, j), mode);
        if (ret < 0) {
            return ret;
        }
    }

    ret = ::mkdir(path.c_str(), mode);
    if (0 == ret) {
        return 0;
    } else {
        if (errno == EEXIST) {
            return 0;
        } else {
            return -1;
        }
    }
}

int RemoveDirAll(const char *name) {
    struct stat st;
    DIR *dir;
    struct dirent *de;
    int fail = 0;

    if (lstat(name, &st) < 0) {
        return -1;
    }

    if (!S_ISDIR(st.st_mode)) {
        return remove(name);
    }

    dir = opendir(name);
    if (dir == NULL) {
        return -1;
    }

    errno = 0;
    while ((de = readdir(dir)) != NULL) {
        char dn[PATH_MAX];
        if (!strcmp(de->d_name, "..") || !strcmp(de->d_name, ".")) {
            continue;
        }
        sprintf(dn, "%s/%s", name, de->d_name);
        if (RemoveDirAll(dn) < 0) {
            fail = 1;
            break;
        }
        errno = 0;
    }
    if (fail || errno < 0) {
        int save = errno;
        closedir(dir);
        errno = save;
        return -1;
    }

    if (closedir(dir) < 0) {
        return -1;
    }

    return rmdir(name);
}

void AnnotateThread(pthread_t handle, const char *name) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    pthread_setname_np(handle, name);
#endif
#endif
}

int ParseBytesValue(const char* str, int64_t* value) {
    char *end = nullptr;
    errno = 0;
    long v = strtol(str, &end, 10);
    if (errno != 0) {
        return -1;
    }

    if (end == NULL || *end == '\0') {
        *value = v;
    } else if (*end == 'k' || *end == 'K') {
        *value = v * 1024;
    } else if (*end == 'm' || *end == 'M') {
        *value = v * 1024 * 1024;
    }  else if (*end == 'g' || *end == 'G') {
        *value = v * 1024 * 1024 * 1024;
    }  else if (*end == 't' || *end == 'T') {
        *value = v * 1024 * 1024 * 1024 * 1024;
    }  else if (*end == 'p' || *end == 'P') {
        *value = v * 1024 * 1024 * 1024 * 1024 * 1024;
    } else {
        errno = EINVAL;
        return -1;
    }
    return 0;
}



static size_t commonPrefixLen(const std::string& left, const std::string& right) {
    size_t common_len = 0;
    auto len = std::min(left.size(), right.size());
    for (size_t i = 0; i < len; ++i) {
        if (left[i] == right[i]) {
            ++common_len;
        } else {
            break;
        }
    }
    return common_len;
}

static uint64_t approximateInt(const std::string& str, size_t offset) {
    uint64_t val = 0;
    for (int i = 0; i < 8; i++) {
        auto ch = offset < str.size() ?
                static_cast<uint8_t>(str[offset]) :
                static_cast<uint8_t>(0);
        val <<= 8;
        val += ch;
        ++offset;
    }
    return val;
}

static std::string approximateStr(uint64_t value) {
    std::string result;
    result.push_back(static_cast<char>(value >> 56));
    result.push_back(static_cast<char>(value >> 48));
    result.push_back(static_cast<char>(value >> 40));
    result.push_back(static_cast<char>(value >> 32));
    result.push_back(static_cast<char>(value >> 24));
    result.push_back(static_cast<char>(value >> 16));
    result.push_back(static_cast<char>(value >> 8));
    result.push_back(static_cast<char>(value));
    return result;
}

std::string FindMiddle(const std::string& left, const std::string& right) {
    if (left >= right) {
        return "";
    }
    auto common_len = commonPrefixLen(left, right);
    auto left_val = static_cast<double>(approximateInt(left, common_len));
    auto right_val = static_cast<double>(approximateInt(right, common_len));
    auto mid_val = static_cast<uint64_t>((left_val + right_val) / 2);

    std::string result;
    if (common_len > 0) {
        result.assign(left.begin(), left.begin() + common_len);
    }
    result += approximateStr(mid_val);
    while (!result.empty() && result.back() == '\0') {
        result.pop_back();
    }

    return result;
}

} /* namespace sharkstore */
