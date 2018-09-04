package blob_store

import (
	"master-server/engine/errors"
	"fmt"
)

const (
	Blob_Header_Size     uint64 = 30
	Blob_Footer_Size     uint64 = 32
	Blob_Key_Header_Size uint64 = 32

	KErrorMessage_Header        = "Error while decoding blob log header"
	KErrorMessage_Record        = "Error while decoding blob log record"
	KErrorMessage_Footer        = "Error while decoding blob log footer"
)

type ExpirationRange struct {
	first  uint64
	second uint64
}

// Format of blob log file header (30 bytes):
//
//    +--------------+---------+---------+-------+-------------+-------------------+
//    | magic number | version |  cf id  | flags | compression | expiration range  |
//    +--------------+---------+---------+-------+-------------+-------------------+
//    |   Fixed32    | Fixed32 | Fixed32 | char  |    char     | Fixed64   Fixed64 |
//    +--------------+---------+---------+-------+-------------+-------------------+
//
type BlobLogHeader struct {
	magicNumber     uint32
	version         uint32
	columnFamilyId  uint32
	flags           uint8
	compression     CompressionType
	hasTtl          bool
	expirationRange ExpirationRange
}

func (header *BlobLogHeader) DecodeForm(src []byte) error {
	if uint64(len(src)) != Blob_Header_Size {
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Header, "Unexpected blob file header size"))
	}

	if !GetUint32(&src, &header.magicNumber) || !GetUint32(&src, &header.version) || !GetUint32(&src, &header.columnFamilyId) {
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Header, "Error decoding magic number, version and column family id"))
	}

	if header.magicNumber != kMagicNumber {
		//[55 143 36 0]
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Header, "Magic number mismatch"))
	}

	if header.version != kVersion1 {
		//[1 0 0 0]
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Header, "Unknown header version"))
	}

	header.flags = src[0]
	header.compression = GetCompressionType(src[1])
	header.hasTtl = (header.flags & 1) == 1
	src = src[2:]
	var eR ExpirationRange
	if !GetUint64(&src, &eR.first) || !GetUint64(&src, &eR.second) {
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Header, "Error decoding expiration range"))
	}
	header.expirationRange = eR
	return nil
}

// Format of blob log file footer (32 bytes):
//
//    +--------------+------------+-------------------+------------+
//    | magic number | blob count | expiration range  | footer CRC |
//    +--------------+------------+-------------------+------------+
//    |   Fixed32    |  Fixed64   | Fixed64 + Fixed64 |   Fixed32  |
//    +--------------+------------+-------------------+------------+
//

type BlobLogFooter struct {
	magicNumber     uint32
	blobCount       uint64
	expirationRange ExpirationRange
	footCRC         uint32
}

func (footer *BlobLogFooter) DecodeForm(slice []byte) error {
	if uint64(len(slice)) != Blob_Footer_Size {
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Footer, "Unexpected blob file header size"))
	}

	//var uint32 magic_number
	//var flags uint8
	//if
	return nil
}

// Blob record format (32 bytes header + key + value):
//
//    +------------+--------------+------------+------------+----------+---------+-----------+
//    | key length | value length | expiration | header CRC | blob CRC |   key   |   value   |
//    +------------+--------------+------------+------------+----------+---------+-----------+
//    |   Fixed64  |   Fixed64    |  Fixed64   |  Fixed32   | Fixed32  | key len | value len |
//    +------------+--------------+------------+------------+----------+---------+-----------+
//
// If file has has_ttl = false, expiration field is always 0, and the blob
// doesn't has expiration.
//
// Also note that if compression is used, value is compressed value and value
// length is compressed value length.
//
// Header CRC is the checksum of (key_len + val_len + expiration), while
// blob CRC is the checksum of (key + value).
//
// We could use variable length encoding (Varint64) to save more space, but it
// make reader more complicated.

type BlobLogRecord struct {
	keySize    uint64
	valueSize  uint64
	expiration uint64
	headerCRC  uint32
	blobCRC    uint32
	key        []byte
	value      []byte
}

func (record *BlobLogRecord) getRecordSize() uint64 {
	return Blob_Key_Header_Size + record.keySize + record.valueSize
}

func (record *BlobLogRecord) DecodeHeaderFrom(src []byte) error {
	if uint64(len(src)) != Blob_Key_Header_Size {
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Record, "Unexpected blob record header size"))
	}

	if !GetUint64(&src, &record.keySize) || !GetUint64(&src, &record.valueSize) ||
		!GetUint64(&src, &record.expiration) || !GetUint32(&src, &record.headerCRC) ||
		!GetUint32(&src, &record.blobCRC) {
		return errors.New(fmt.Sprintf("%s, %s", KErrorMessage_Record, "Error decoding content"))
	}

	return nil
}

func (record *BlobLogRecord) GetKey() []byte {
	return record.key
}

func (record *BlobLogRecord) GetVaule() []byte {
	return record.value
}