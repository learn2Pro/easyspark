package raft

import (
	"encoding/binary"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type KvStoreLevelDB struct {
	path string
	db   *leveldb.DB
}

func MakeLevelDBKvStore(dbPath string) (*KvStoreLevelDB, error) {
	leveldb, err := leveldb.OpenFile(dbPath, &opt.Options{})
	if err != nil {
		return nil, err
	}
	return &KvStoreLevelDB{
		path: dbPath,
		db:   leveldb,
	}, nil
}

func (ldb *KvStoreLevelDB) Put(k []byte, v []byte) error {
	return ldb.db.Put(k, v, nil)
}

func (ldb *KvStoreLevelDB) Get(k []byte) ([]byte, error) {
	return ldb.db.Get(k, nil)
}

func (ldb *KvStoreLevelDB) Del(k []byte) error {
	return ldb.db.Delete(k, nil)
}

func (ldb *KvStoreLevelDB) GetPrefixRangeKvs(prefix []byte) ([]string, []string, error) {
	keys := make([]string, 0)
	vals := make([]string, 0)
	iter := ldb.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		DPrintf("leveldb iter key -> %v, val -> %v", iter.Key(), iter.Value())
		keys = append(keys, string(iter.Key()))
		vals = append(vals, string(iter.Value()))
	}
	iter.Release()
	return keys, vals, nil
}

func (levelDB *KvStoreLevelDB) SeekPrefixLast(prefix []byte) ([]byte, []byte, error) {
	iter := levelDB.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	ok := iter.Last()
	var keyBytes, valBytes []byte
	if ok {
		keyBytes = iter.Key()
		valBytes = iter.Value()
	}
	return keyBytes, valBytes, nil
}

func (levelDB *KvStoreLevelDB) SeekPrefixKeyIdMax(prefix []byte) (uint64, error) {
	iter := levelDB.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	var maxKeyId uint64
	maxKeyId = 0
	if iter.Last() {
		kBytes := iter.Key()
		KeyId := binary.LittleEndian.Uint64(kBytes[len(prefix):])
		if KeyId > maxKeyId {
			maxKeyId = KeyId
		}
	} else {
		return maxKeyId, iter.Error()
	}
	// lastEnt := iter.Last()
	// for iter.Next() {
	// 	if iter.Error() != nil {
	// 		return maxKeyId, iter.Error()
	// 	}
	// 	kBytes := iter.Key()
	// 	KeyId := binary.LittleEndian.Uint64(kBytes[len(prefix):])
	// 	if KeyId > maxKeyId {
	// 		maxKeyId = KeyId
	// 	}
	// }
	return maxKeyId, nil
}

func (levelDB *KvStoreLevelDB) SeekPrefixFirst(prefix string) ([]byte, []byte, error) {
	iter := levelDB.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()
	if iter.Next() {
		return iter.Key(), iter.Value(), nil
	}
	return []byte{}, []byte{}, errors.New("seek not find key")
}

func (levelDB *KvStoreLevelDB) DelPrefixKeys(prefix string) error {
	iter := levelDB.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		err := levelDB.db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	iter.Release()
	return nil
}

func (levelDB *KvStoreLevelDB) DumpPrefixKey(prefix string) (map[string]string, error) {
	kvs := make(map[string]string)
	iter := levelDB.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		k := string(iter.Key())
		v := string(iter.Value())
		kvs[k] = v
	}
	iter.Release()
	return kvs, iter.Error()
}
