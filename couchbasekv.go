package couchbasekv

import (
	"fmt"
	"strings"

	"plugins"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/op/go-logging"
	json "github.com/orofarne/strict-json"
)

var log = logging.MustGetLogger("global")

type couchbaseKVConf struct {
	Addrs   []string
	Pool    string
	Bucket  string
	Expire  int
	Retries int
}

type CouchbaseKV struct {
	config couchbaseKVConf
	bucket *couchbase.Bucket
}

type CouchbaseKVFactory struct {
}

func (self *CouchbaseKVFactory) Name() string {
	return "CouchbaseKVPlugin"
}

func (self *CouchbaseKVFactory) New(cfg json.RawMessage) (interface{}, error) {
	var res = new(CouchbaseKV)
	err := res.Configure(cfg)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func init() {
	plugins.DefaultPluginStore.AddPlugin(new(CouchbaseKVFactory))
}

func (self *CouchbaseKV) Configure(cfg json.RawMessage) error {
	// Unmarshal config
	err := json.Unmarshal(cfg, &self.config)
	if err != nil {
		return err
	}

	// Defaults
	if self.config.Pool == "" {
		self.config.Pool = "default"
	}

	// Connect
	var errs []error
	for _, addr := range self.config.Addrs {
		self.bucket, err = couchbase.GetBucket(addr, self.config.Pool, self.config.Bucket)
		if err == nil {
			return nil
		} else {
			errs = append(errs, err)
		}
	}
	return fmt.Errorf("Failed to connect to couchbase: %v", errs)
}

func (self *CouchbaseKV) Get(key string) (data []byte, err error) {
	log.Debug("Request data by key '%v' from couchbase...", key)
	for i := 0; i < self.config.Retries+1; i++ {
		if i != 0 {
			log.Debug("Retry request tile by key '%v' from couchbase...", key)
		}
		data, err = self.bucket.GetRaw(key)
		if err == nil {
			return
		}
		if err != nil && strings.Contains(err.Error(), "KEY_ENOENT") {
			log.Debug("Key '%v' not found", key)
			return nil, nil
		}
	}
	return
}

func (self *CouchbaseKV) Set(key string, value []byte) (err error) {
	log.Debug("Save data by key '%v' to couchbase", key)
	for i := 0; i < self.config.Retries+1; i++ {
		if i != 0 {
			log.Debug("Retry save data by key '%v' to couchbase", key)
		}
		err = self.bucket.SetRaw(key, self.config.Expire, value)
		if err == nil {
			return
		}
	}
	return
}

func (self *CouchbaseKV) Delete(key string) (err error) {
	log.Debug("Delete data by key '%v' from couchbase", key)
	for i := 0; i < self.config.Retries+1; i++ {
		if i != 0 {
			log.Debug("Retry delete data by key '%v' from couchbase", key)
		}
		err = self.bucket.Delete(key)
		if err == nil || strings.Contains(err.Error(), "KEY_ENOENT") {
			return nil
		}
	}
	return

}
