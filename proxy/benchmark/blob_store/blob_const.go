package blob_store

import (
	"math"
	"encoding/binary"
)

const kMagicNumber uint32 = 2395959 // 0x00248f37
const kVersion1 uint32 = 1
const kNoExpiration uint64 = math.MaxUint64 // ^uint64(0); 1 << 64 -1; (1 << 63 -1) << 1 + 1

type CompressionType uint8

const (
	KNOCompression     CompressionType = iota
	KSnappyCompression CompressionType = 1
	KZlibCompression   CompressionType = 2
	KBZip2Compression  CompressionType = 3
	KLZ4Compression    CompressionType = 4
	KXpressCompression CompressionType = 5
)

func GetCompressionType(compression uint8) CompressionType {
	switch compression {
	case 1:
		return KSnappyCompression
	case 2:
		return KZlibCompression
	case 3:
		return KBZip2Compression
	case 4:
		return KLZ4Compression
	case 5:
		return KXpressCompression
	default:
		return KNOCompression
	}
}

func ByteToUint32(data []byte) uint32 {
	return uint32(binary.LittleEndian.Uint32(data))
}

func ByteToUint64(data []byte) uint64 {
	return uint64(binary.LittleEndian.Uint64(data))
}

func GetUint32(slice *[]byte, value *uint32) bool {
	if len(*slice) < 4 {
		return false
	}
	*value = ByteToUint32(*slice)
	*slice = (*slice)[4:]
	return true
}

func GetUint64(slice *[]byte, value *uint64) bool {
	if len(*slice) < 8 {
		return false
	}
	*value = ByteToUint64(*slice)
	*slice = (*slice)[8:]
	return true
}
