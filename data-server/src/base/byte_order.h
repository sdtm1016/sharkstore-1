_Pragma("once");

#ifdef __linux__
#include <endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define htobe16(x) OSSwapInt16(x)
#define htole16(x) (x)
#define be16toh(x) OSSwapInt16(x)
#define le16toh(x) (x)

#define htobe32(x) OSSwapInt32(x)
#define htole32(x) (x)
#define be32toh(x) OSSwapInt32(x)
#define le32toh(x) (x)

#define htobe64(x) OSSwapInt64(x)
#define htole64(x) (x)
#define be64toh(x) OSSwapInt64(x)
#define le64toh(x) (x)

#elif __BYTE_ORDER == __BIG_ENDIAN
#define htobe16(x) (x)
#define htole16(x) OSSwapInt16(x)
#define be16toh(x) (x)
#define le16toh(x) OSSwapInt16(x)

#define htobe32(x) (x)
#define htole32(x) OSSwapInt32(x)
#define be32toh(x) (x)
#define le32toh(x) OSSwapInt32(x)

#define htobe64(x) (x)
#define htole64(x) OSSwapInt64(x)
#define be64toh(x) (x)
#define le64toh(x) OSSwapInt64(x)

#else
#error unknown machine byte order
#endif

#else
#error unsupported platform
#endif
