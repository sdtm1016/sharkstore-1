package server

import (
	"testing"
)

func TestClusterIDGenerator(t *testing.T) {
	initDataPath()
	cfg := NewDefaultConfig()
	store := mockRaftServer(cfg, t)
	idGenerator1 := NewClusterIDGenerator(store)
	//before generate ,value is []
	getKey(t, store)
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := idGenerator1.GenID()
			if err != nil {
				t.Errorf("err:%v", id)
			} else {
				t.Logf("id: %v", id)
			}

		}()
	}
	wg.Wait()
	getKey(t, store)

}

func TestClusterIdsGenerator(t *testing.T) {
	initDataPath()
	cfg := NewDefaultConfig()
	store := mockRaftServer(cfg, t)
	idGenerator1 := NewClusterIDGenerator(store)
	//before generate ,value is []
	getKey(t, store)
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids, err := idGenerator1.GetBatchIds(uint32(i))
			if err != nil {
				t.Errorf("errs:%v", err)
			} else {
				t.Logf("i: %v, ids: [%v]", i, ids)
			}
	//
		}(i)
	}
	wg.Wait()
	getKey(t, store)

}

func getKey(t *testing.T, store Store) {
	value, err := store.Get([]byte(AUTO_INCREMENT_ID))
	if err != nil {
		if err == ErrNotFound {
			t.Fatalf("not found")
		}
		t.Fatalf("error %v", err)
	}
	t.Logf("value is %d", value)

	if value != nil {
		end, err := bytesToUint64(value)
		if err != nil {
			t.Fatalf("format value erro %v", err)
		}
		t.Logf("value is %d", end)
	} else {
		t.Logf("value is nil")
	}
}
