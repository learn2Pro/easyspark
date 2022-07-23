package raft

type KvStore interface {
	Put(key []byte, val []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) error
	GetPrefixRangeKvs(prefix []byte) ([]string, []string, error)
	SeekPrefixLast(prefix []byte) ([]byte, []byte, error)
	SeekPrefixFirst(prefix string) ([]byte, []byte, error)
	DelPrefixKeys(prefix string) error
	SeekPrefixKeyIdMax(prefix []byte) (uint64, error)
	DumpPrefixKey(string) (map[string]string, error)
}

func KvStoreFactory(engineName string, path string) KvStore {
	switch engineName {
	case "leveldb":
		ldb, err := MakeLevelDBKvStore(path)
		if err != nil {
			DPrintf("make leveldb kv store err: %s", err)
			panic(err)
		}
		return ldb
	}
	return nil
}
