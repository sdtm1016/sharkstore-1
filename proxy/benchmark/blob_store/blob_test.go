package blob_store

import (
	"testing"
	"fmt"
	"path/filepath"
	"encoding/binary"
)

func Test_Blob_Reader(t *testing.T) {
	path := "/Users/wumeijun/Documents"

	dirList, err := GetFiles(path)
	if err != nil {
		t.Fatal("error: %v", err)
	}
	for _, dir := range dirList {
		fileName := fmt.Sprintf("%s%s%s", path, string(filepath.Separator), dir)
		reader, err := NewReader(fileName)
		if err != nil {
			t.Logf("new reader %v error: %v", fileName, err)
			return
		}
		err = reader.ReadHeaderMock()
		if err != nil {
			t.Errorf("read header for blob-file err: %v", err)
			return
		}
		records := reader.ReadRecords()
		//for _, r := range records {
		//	t.Logf("key:%s, value:%s", string(r.key) , string(r.value))
		//}
		t.Logf("key:%d, \n, value:%s", records[0].key, records[0].value)
		t.Logf("key:%s, value:%s", string(records[0].key) , string(records[0].value))
	}


}


func Test_Byte(t *testing.T)  {
	bits := kMagicNumber
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	t.Logf("%v", bytes)

	ss := uint32(binary.LittleEndian.Uint32(bytes))
	t.Logf("ss %v", ss)

	headerByte := bytes[0:2]
	updateByte(headerByte)
	t.Logf("%v", bytes)

	bits2 := kVersion1
	bytes2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes2, bits2)
	t.Logf("%v", bytes2)

	ss2 := uint32(binary.LittleEndian.Uint32(bytes2))
	t.Logf("ss %v", ss2)

	headerCrc :=  uint32(2765389801)
	bytes3 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes3, headerCrc)
	t.Logf("%v", bytes3)
}

func updateByte(bb []byte)  {
	bb[0] = 22
	bb[1] = 33
}