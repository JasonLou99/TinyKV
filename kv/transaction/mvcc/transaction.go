package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
// MvccTxn将写入作为单个事务的一部分分组在一起。它还提供了对底层的抽象
// 存储，将时间戳、写入和锁定的概念降低到纯键和值中。
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfWrite,
			Key:   EncodeKey(key, txn.StartTS),
			Value: write.ToBytes(),
		},
	})
	log.Info("PutWrite()")
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	lock, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		res, err := ParseLock(lock)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	// wb := &engine_util.WriteBatch{}
	// // 将lock写入lockCF，value是lock.ToBytes
	// // key: key+CfLock, value: lock.ToBytes
	// wb.SetCF(engine_util.CfLock, key, lock.ToBytes())

	// 将锁写入事务的writes中
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfLock,
			Key:   key,
			Value: lock.ToBytes(),
		},
	})
	// log.Info("")

}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	// DeleteLock是用于Delete操作的Lock，而非删除Lock
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfLock,
			Key: key,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
// 在此事务开始之前提交的最近的值。
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// key的排序：UserKey升序，再按照TS降序
	// 所以从EncodeKey(key, txn.StartTS)，开始就相当于在key的所有版本中，按时间倒序遍历出所有的key
	// CfWrite中的txn.StartTS是指该UserKey的CommitTS
	// writeCF的迭代器
	it := txn.Reader.IterCF(engine_util.CfWrite)
	log.Info("txn.StartTS: ", txn.StartTS)
	var startTS uint64
	for it.Seek(EncodeKey(key, txn.StartTS)); it.Valid(); it.Next() {
		// 此时key2 是UserKey+CommitTS
		key2 := it.Item().Key()
		log.Info(key2)
		// 找到想要取值的UserKey
		if bytes.Equal(DecodeUserKey(key2), key) {
			// CommitTS < txn.StartTS
			commitTs := decodeTimestamp(key2)
			log.Info("txn.CommitTS: ", commitTs)
			if commitTs <= txn.StartTS {
				// 再根据UserKey和write内的StartTS找到实际key的value
				val, err := it.Item().Value()
				if err != nil {
					return nil, err
				}
				write, err := ParseWrite(val)
				if err != nil {
					return nil, err
				}
				startTS = write.StartTS
				log.Info("txn.StartTS: ", startTS)
				return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, startTS))
			}
		}
	}
	// return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, startTS))
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfDefault,
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfDefault,
			Key: EncodeKey(key, txn.StartTS),
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	var startTS uint64
	// Seek(key)：[]byte{16, 240}没有加时间戳，可以遍历所有该UserKey的kv对
	for it.Seek(key); it.Valid(); it.Next() {
		key2 := it.Item().Key()
		if bytes.Equal(DecodeUserKey(key2), key) {
			// CommitTS < txn.StartTS
			commitTs := decodeTimestamp(key2)
			log.Info("CommitTS: ", commitTs)
			val, err := it.Item().Value()
			if err != nil {
				return nil, 0, err
			}
			write, err := ParseWrite(val)
			if err != nil {
				return nil, 0, err
			}
			startTS = write.StartTS
			log.Info("StartTS: ", startTS)
			if startTS == txn.StartTS {
				return write, commitTs, nil
			}
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 给定key的最近(StartTS最大)的write，返回事务write和其commitTS
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	var startTS uint64
	var resultWrite *Write
	var resultTs uint64
	var maxStartTS uint64
	for it.Seek(key); it.Valid(); it.Next() {
		key2 := it.Item().Key()
		if bytes.Equal(DecodeUserKey(key2), key) {
			// CommitTS < txn.StartTS
			commitTs := decodeTimestamp(key2)
			log.Info("CommitTS: ", commitTs)
			// if commitTs <= txn.StartTS {
			// 再根据UserKey和write内的StartTS找到实际key的value
			val, err := it.Item().Value()
			if err != nil {
				return nil, 0, err
			}
			write, err := ParseWrite(val)
			if err != nil {
				return nil, 0, err
			}
			startTS = write.StartTS
			log.Info("StartTS: ", startTS)
			if startTS > maxStartTS {
				resultWrite = write
				resultTs = commitTs
				maxStartTS = startTS
			}
			// }
		}
	}
	return resultWrite, resultTs, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
