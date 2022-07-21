package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	key := req.Key
	cf := req.Cf
	// reader, err := server.storage.Reader(req.GetContext())
	reader, err := server.storage.Reader(nil)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	rawGetResp := &kvrpcpb.RawGetResponse{}
	result, err := reader.GetCF(cf, key)
	if err != nil {
		log.Error(err)
		// rawGetResp.Error = err.Error()
		return nil, err
	}
	if len(result) == 0 {
		rawGetResp.NotFound = true
	}
	rawGetResp.Value = result
	return rawGetResp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key := req.Key
	cf := req.Cf
	value := req.Value
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   key,
				Cf:    cf,
				Value: value,
			},
		},
	}
	// for i := range key {
	// 	put, ok := batch[i].Data.(storage.Put)
	// 	if ok {
	// 		put.Cf = cf
	// 		put.Key = key
	// 		put.Value = value
	// 	}
	// }
	server.storage.Write(nil, batch)
	rawPutResp := &kvrpcpb.RawPutResponse{}
	return rawPutResp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key := req.Key
	cf := req.Cf
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key: key,
				Cf:  cf,
			},
		},
	}
	server.storage.Write(nil, batch)
	rawDeleteResp := &kvrpcpb.RawDeleteResponse{}
	return rawDeleteResp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey := req.StartKey
	limit := req.Limit
	cf := req.Cf
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	it := sr.IterCF(cf)
	count := 0
	var result []*kvrpcpb.KvPair
	for it.Seek(startKey); it.Valid(); it.Next() {
		if count == int(limit) {
			break
		}
		item := it.Item()
		value, err := item.Value()
		if err != nil {
			count++
			continue
		}
		kv := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		}
		result = append(result, kv)
		count++
	}
	rawScanResp := &kvrpcpb.RawScanResponse{}
	rawScanResp.Kvs = result
	log.Info(len(result))
	return rawScanResp, nil
}
