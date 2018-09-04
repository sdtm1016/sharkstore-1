package blob_store

import (
	"fmt"
	"path/filepath"
	"master-server/engine/errors"
	"util/log"
)

func CheckKey(path string, keysMap map[string]uint8) error {
	dirList, err := GetFiles(path)
	if err != nil {
		return err
	}

	var hit uint32
	for _, dir := range dirList {
		fileName := fmt.Sprintf("%s%s%s", path, string(filepath.Separator), dir)
		reader, err := NewReader(fileName)
		if err != nil {
			return err
		}
		err = reader.ReadHeaderMock()
		if err != nil {
			return errors.New(fmt.Sprintf("read header for blob-file err: %v", err))
		}
		records := reader.ReadRecords()
		for _, r := range records {
			if _, ok := keysMap[string(r.key)]; ok {
				//todo check value
				log.Warn("still exist key : %v; file: %v", string(r.key), fileName)
				hit++
			}
		}
	}

	if hit != 0 {
		return errors.New(fmt.Sprintf("check key number: %v, still exist key number: %v", len(keysMap), hit))
	}
	return nil
}
