package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// StandAloneStorage是单节点TinyKV实例的“存储”实现。
// 它不与其他节点通信，所有数据都存储在本地。
type StandAloneStorage struct {
	// Your Data Here (1).
	// 底层badgerDB
	Kv *badger.DB
	// badgerDB的配置信息
	Conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	log.Infof("here is NewStandAloneStorage()")
	standAloneStorage := &StandAloneStorage{
		Conf: conf,
		Kv:   nil,
	}
	return standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Info("here is Start() in StandAloneStorage")
	opts := badger.DefaultOptions
	opts.Dir = s.Conf.DBPath
	opts.ValueDir = s.Conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Error(err)
		return err
	}
	s.Kv = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.Kv.Close(); err != nil {
		return err
	}
	return nil
}

// 返回一个用来get和scan的StorageReader结构体
// StorageReader是一个接口，就是要返回它实现类的实体
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	sr := StandAloneStorageReader{
		// Txn: &badger.Txn{},
		// For read-only transactions, set update arg to false
		Txn: s.Kv.NewTransaction(false),
	}
	return &sr, nil
}

// StorageReader实现类
type StandAloneStorageReader struct {
	Txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	log.Info("here is GetCF() in StandAloneStorage")
	val, err := engine_util.GetCFFromTxn(sr.Txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			// key为空不视为错误
			log.Info(err)
			val = []byte(nil)
			return val, nil
		}
		log.Error(err)
		return nil, err
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	log.Info("here is IterCF() in StandAloneStorage")
	it := engine_util.NewCFIterator(cf, sr.Txn)
	defer it.Rewind()
	return it
}

func (sr *StandAloneStorageReader) Close() {
	// 事务关闭
	sr.Txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	log.Info("here is Write() in StandAloneStorage")
	wb := &engine_util.WriteBatch{}
	for _, singleOp := range batch {
		// 将Data转为storage.Put
		put, ok := singleOp.Data.(storage.Put)
		if ok {
			wb.SetCF(put.Cf, put.Key, put.Value)
		}
		// delete操作
		delete, ok := singleOp.Data.(storage.Delete)
		if ok {
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}
	return wb.WriteToDB(s.Kv)
}
