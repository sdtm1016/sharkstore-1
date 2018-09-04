package blob_store

import (
	"os"
	"io/ioutil"
	"master-server/engine/errors"
	"util/log"
	"strings"
)

const (
	kReadHeader        = iota
	kReadHeaderKey
	kReadHeaderKeyBlob
)

type ReadLevel int

func getReadLevel(level int) ReadLevel {
	switch level {
	case 1:
		{
			return kReadHeader
		}
	case 2:
		{
			return kReadHeaderKey
		}
	case 3:
		{
			return kReadHeaderKeyBlob
		}
	}
	return 0
}

type Reader struct {
	data     []byte
	nextByte uint64
}

func NewReader(path string) (*Reader, error) {
	byteData, err := ReadAll(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		data:     byteData[:],
		nextByte: 0},
		nil
}

func (r *Reader) ResetNextByte() {
	r.nextByte = 0
}

func (r *Reader) GetNextByte() uint64 {
	return r.nextByte
}

func (r *Reader) ReadHeader(header *BlobLogHeader) error {
	headerByte, ok := r.ReadSlice(Blob_Header_Size)
	if !ok {
		return errors.New("read slice error")
	}
	return header.DecodeForm(headerByte)
	return nil
}

func (r *Reader) ReadHeaderMock() error {
	_, ok := r.ReadSlice(Blob_Header_Size)
	if !ok {
		return errors.New("read slice error")
	}
	return nil
}

func (r *Reader) ReadRecord (record *BlobLogRecord) error {
	headerByte, ok := r.ReadSlice(Blob_Key_Header_Size)
	if !ok {
		return errors.New("read slice error")
	}
	err := record.DecodeHeaderFrom(headerByte)
	if err != nil {
		return errors.New("decode record header error")
	}
	key, ok1 := r.ReadSlice(record.keySize)
	value, ok2 := r.ReadSlice(record.valueSize)
	if !ok1 || !ok2 {
		return errors.New("read record key or value slice error")
	}
	record.key = key
	record.value = value
	return nil
}

func (r *Reader) ReadRecords() []BlobLogRecord {
	var records []BlobLogRecord
	for {
		var record BlobLogRecord
		if err := r.ReadRecord(&record);  err != nil {
			//todo
			break
		} else {
			records = append(records, record)
		}
	}
	return records
}



func (r *Reader) ReadFooter(footer *BlobLogFooter) {

	//footerByte := s[len(s)-int(footer.getHeaderSize()):]

}

func (r *Reader) ReadSlice(size uint64) ([]byte, bool) {
	if uint64(len(r.data)) < (r.nextByte + size){
		log.Warn("read slice len less than need size %v", size)
		return nil, false
	}
	buffer := make([]byte, size)
	copy(buffer, r.data[r.nextByte: r.nextByte + size])
	r.nextByte = r.nextByte + size
	return buffer, true
}

func ReadAll(filePth string) ([]byte, error) {
	f, err := os.Open(filePth)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}


func GetFiles(directory string) ([]string, error) {
	dirList, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var fileNames []string
	for _, f := range dirList {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".blob"){
			fileNames = append(fileNames, f.Name())
		}
	}
	return fileNames, nil

}