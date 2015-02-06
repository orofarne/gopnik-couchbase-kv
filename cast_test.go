package couchbasekv

import (
	"gopnik"
	"testing"
)

func TestFilecacheCast(t *testing.T) {
	fp := new(CouchbaseKV)
	var v gopnik.KVStore
	v = fp
	_ = v
}
