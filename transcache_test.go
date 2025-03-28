/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.
*/

package ltcache

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRemKey(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("t11_", "mm", "test", nil, true, "")
	if t1, ok := tc.Get("t11_", "mm"); !ok || t1 != "test" {
		t.Error("Error setting cache: ", ok, t1)
	}
	tc.Remove("t11_", "mm", true, "")
	if t1, ok := tc.Get("t11_", "mm"); ok || t1 == "test" {
		t.Error("Error removing cached key")
	}
}

func TestTransaction(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("mmm_", "t11", "test", nil, false, transID)
	if t1, ok := tc.Get("mmm_", "t11"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	tc.Set("mmm_", "t12", "test", nil, false, transID)
	tc.Remove("mmm_", "t11", false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("mmm_", "t12"); !ok || t1 != "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("mmm_", "t11"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}

}

func TestTransactionRemove(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("t21_", "mm", "test", nil, false, transID)
	tc.Set("t21_", "nn", "test", nil, false, transID)
	tc.Remove("t21_", "mm", false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("t21_", "mm"); ok || t1 == "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("t21_", "nn"); !ok || t1 != "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}
}

func TestTransactionRemoveGroup(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("t21_", "mm", "test", []string{"grp1"}, false, transID)
	tc.Set("t21_", "nn", "test", []string{"grp1"}, false, transID)
	tc.RemoveGroup("t21_", "grp1", false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("t21_", "mm"); ok || t1 == "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("t21_", "nn"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}
}

func TestTransactionRollback(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Set("aaa_", "t31", "test", nil, false, transID)
	if t1, ok := tc.Get("aaa_", "t31"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	tc.Set("aaa_", "t32", "test", nil, false, transID)
	if _, hasTransID := tc.transactionBuffer[transID]; !hasTransID {
		t.Error("Does not have transactionID")
	}
	tc.RollbackTransaction(transID)
	if t1, ok := tc.Get("aaa_", "t32"); ok || t1 == "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("aaa_", "t31"); ok || t1 == "test" {
		t.Error("Error in transaction cache")
	}
	if _, hasTransID := tc.transactionBuffer[transID]; hasTransID {
		t.Error("Should not longer have transactionID")
	}
}

func TestTransactionRemBefore(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	transID := tc.BeginTransaction()
	tc.Remove("t41_", "mm", false, transID)
	tc.Remove("t41_", "nn", false, transID)
	tc.Set("t41_", "mm", "test", nil, false, transID)
	tc.Set("t41_", "nn", "test", nil, false, transID)
	tc.CommitTransaction(transID)
	if t1, ok := tc.Get("t41_", "mm"); !ok || t1 != "test" {
		t.Error("Error commiting transaction")
	}
	if t1, ok := tc.Get("t41_", "nn"); !ok || t1 != "test" {
		t.Error("Error in transaction cache")
	}
}

func TestTCGetGroupItems(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("xxx_", "t1", "test", []string{"grp1"}, true, "")
	tc.Set("xxx_", "t2", "test", []string{"grp1"}, true, "")
	if grpItms := tc.GetGroupItems("xxx_", "grp1"); len(grpItms) != 2 {
		t.Errorf("Received group items: %+v", grpItms)
	}
	if grpItms := tc.GetGroupItems("xxx_", "nonexsitent"); grpItms != nil {
		t.Errorf("Received group items: %+v", grpItms)
	}
}

func TestRemGroup(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("xxx_", "t1", "test", []string{"grp1"}, true, "")
	tc.Set("xxx_", "t2", "test", []string{"grp1"}, true, "")
	tc.RemoveGroup("xxx_", "grp1", true, "")
	_, okt1 := tc.Get("xxx_", "t1")
	_, okt2 := tc.Get("xxx_", "t2")
	if okt1 || okt2 {
		t.Error("Error removing prefix: ", okt1, okt2)
	}
}

func TestCacheCount(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"dst_": {MaxItems: -1},
		"rpf_": {MaxItems: -1}})
	tc.Set("dst_", "A1", "1", nil, true, "")
	tc.Set("dst_", "A2", "2", nil, true, "")
	tc.Set("rpf_", "A3", "3", nil, true, "")
	tc.Set("dst_", "A4", "4", nil, true, "")
	tc.Set("dst_", "A5", "5", nil, true, "")
	if itms := tc.GetItemIDs("dst_", ""); len(itms) != 4 {
		t.Errorf("Error getting item ids: %+v", itms)
	}
}

func TestCacheGetStats(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"part1": {MaxItems: -1},
		"part2": {MaxItems: -1}})
	testCIs := []*cachedItem{
		{itemID: "_1_", value: "one"},
		{itemID: "_2_", value: "two", groupIDs: []string{"grp1"}},
		{itemID: "_3_", value: "three", groupIDs: []string{"grp1", "grp2"}},
		{itemID: "_4_", value: "four", groupIDs: []string{"grp1", "grp2", "grp3"}},
		{itemID: "_5_", value: "five", groupIDs: []string{"grp4"}},
	}
	for _, ci := range testCIs {
		tc.Set("part1", ci.itemID, ci.value, ci.groupIDs, true, "")
	}
	for _, ci := range testCIs[:4] {
		tc.Set("part2", ci.itemID, ci.value, ci.groupIDs, true, "")
	}
	eCs := map[string]*CacheStats{
		"part1": {Items: 5, Groups: 4},
		"part2": {Items: 4, Groups: 3},
	}
	if cs := tc.GetCacheStats(nil); reflect.DeepEqual(eCs, cs) {
		t.Errorf("expecting: %+v, received: %+v", eCs, cs)
	}
}

// Try concurrent read/write of the cache
func TestCacheConcurrent(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"dst_": {MaxItems: -1},
		"rpf_": {MaxItems: -1}})
	s := &struct{ Prefix string }{Prefix: "+49"}
	tc.Set("dst_", "DE", s, nil, true, "")
	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			tc.Get("dst_", "DE")
			wg.Done()
		}()
	}
	s.Prefix = "+491"
	wg.Wait()
}

type TenantID struct {
	Tenant string
	ID     string
}

func (tID *TenantID) CacheClone() any {
	tClone := new(TenantID)
	*tClone = *tID
	return tClone
}

func TestGetClone(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"t11_": {
			MaxItems: -1,
			Clone:    true,
		},
	})
	a := &TenantID{Tenant: "cgrates.org", ID: "ID#1"}
	tc.Set("t11_", "mm", a, nil, true, "")
	if t1, ok := tc.Get("t11_", "mm"); !ok {
		t.Error("Error setting cache: ", ok, t1)
	} else {
		tcCloned := t1.(*TenantID)
		if !reflect.DeepEqual(tcCloned, a) {
			t.Errorf("Expecting: %+v, received: %+v", a, tcCloned)
		}
		a.ID = "ID#2"
		if reflect.DeepEqual(tcCloned, a) {
			t.Errorf("Expecting: %+v, received: %+v", a, tcCloned)
		}
	}
}

func TestGetClone2(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"t11_": {
			MaxItems: -1,
			Clone:    true,
		},
	})
	tc.Set("t11_", "mm", nil, nil, true, "")
	if x, ok := tc.Get("t11_", "mm"); !ok {
		t.Error("Couldnt get cache value, ok: ", ok)
	} else if x != nil {
		t.Errorf("Expecting: nil, received: %+v", x)
	}
}

func TestTranscacheHasGroup(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {},
		},
	}
	chID := "testChID"
	grpID := "testGroupID"

	exp := false
	rcv := tc.HasGroup(chID, grpID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}

	tc.cache[chID].groups = map[string]map[string]struct{}{
		"testGroupID": make(map[string]struct{}),
	}

	exp = true
	rcv = tc.HasGroup(chID, grpID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheGetGroupItemIDs(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				groups: map[string]map[string]struct{}{
					"testGroupID": {
						"testField1": struct{}{},
						"testField2": struct{}{},
					},
				},
			},
		},
	}
	chID := "testChID"
	grpID := "testGroupID"

	exp := []string{"testField1", "testField2"}
	rcv := tc.GetGroupItemIDs(chID, grpID)

	if len(exp) != len(rcv) {
		t.Fatalf("\nexpected slice length: <%+v>, \nreceived slice length: <%+v>",
			len(exp), len(rcv))
	}

	diff := make(map[string]int, len(exp))

	for _, valRcv := range rcv {
		diff[valRcv]++
	}
	for _, valExp := range exp {
		if _, ok := diff[valExp]; !ok {
			t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
		}
		diff[valExp] -= 1
		if diff[valExp] == 0 {
			delete(diff, valExp)
		}
	}
	if len(diff) != 0 {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheClearSpecific(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
					"item3": {},
				},
			},
		},
	}
	chIDs := []string{"testChID2"}

	exp := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
			},
		},
	}

	tc.Clear(chIDs)

	for key, value := range tc.cache {
		if len(value.cache) != len(exp.cache[key].cache) {
			t.Errorf("\nKey: <%+v>\nexpected nr of items: <%+v>, \nreceived nr of items: <%+v>",
				key, len(exp.cache[key].cache), len(value.cache))
		}
	}
}

func TestTranscacheClearAll(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {},
					"item2": {},
					"item3": {},
				},
			},
		},
	}
	var chIDs []string

	exp := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
			},
		},
	}

	tc.Clear(chIDs)

	for key, value := range tc.cache {
		if len(value.cache) != len(exp.cache[key].cache) {
			t.Errorf("\nKey: <%+v>\nexpected nr of items: <%+v>, \nreceived nr of items: <%+v>",
				key, len(exp.cache[key].cache), len(value.cache))
		}
	}
}

func TestTranscacheClearWithOfflineCollector(t *testing.T) {
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {itemID: "item1"},
					"item2": {itemID: "item2"},
				},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: true, ItemID: "item1"},
						"item2": {IsSet: true, ItemID: "item2"},
						"item3": {IsSet: false, ItemID: "item3"},
					},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache: map[string]*cachedItem{
					"item1": {itemID: "item1"},
					"item2": {itemID: "item2"},
					"item3": {itemID: "item3"},
				},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: true, ItemID: "item1"},
						"item2": {IsSet: true, ItemID: "item2"},
					},
				},
			},
		},
	}

	for i := range tc.cache {
		tc.cache[i].onEvicted = append(tc.cache[i].onEvicted, func(itemID string, _ any) {
			tc.cache[i].offCollector.storeRemoveEntity(itemID, 1)
		})
	}

	exp := &TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: false, ItemID: "item1"},
						"item2": {IsSet: false, ItemID: "item2"},
						"item3": {IsSet: false, ItemID: "item3"},
					},
				},
			},
			"testChID2": {
				lruIdx: list.New(),
				ttlIdx: list.New(),
				cache:  map[string]*cachedItem{},
				offCollector: &OfflineCollector{
					logger: &testLogger{log.New(&logBuf, "", 0)},
					collection: map[string]*CollectionEntity{
						"item1": {IsSet: false, ItemID: "item1"},
						"item2": {IsSet: false, ItemID: "item2"},
						"item3": {IsSet: false, ItemID: "item3"},
					},
				},
			},
		},
	}

	tc.Clear(nil)

	for key, value := range tc.cache {
		if len(value.cache) != len(exp.cache[key].cache) {
			t.Errorf("\nKey: <%+v>\nexpected nr of items: <%+v>, \nreceived nr of items: <%+v>",
				key, len(exp.cache[key].cache), len(value.cache))
		}
	}
	for key, c := range tc.cache {
		for collKey, coll := range c.offCollector.collection {
			if !reflect.DeepEqual(exp.cache[key].offCollector.collection[collKey], coll) {
				t.Errorf("Instance <%s>. Expected <%+v>, \nReceived <%+v>", key, exp.cache[key].offCollector.collection[collKey], coll)
			} else if rcv := logBuf.String(); rcv != "" {
				t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
			}
		}
	}
}

func TestTranscacheGetItemExpiryTime(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"testItemID": {
						expiryTime: time.Time{},
					},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	var exp time.Time
	expok := true
	rcv, ok := tc.GetItemExpiryTime(chID, itmID)

	if ok != expok {
		t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", expok, ok)
	}

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheHasItem(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"testItemID": {},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	exp := true
	rcv := tc.HasItem(chID, itmID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheNoItem(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"otherItem": {},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	exp := false
	rcv := tc.HasItem(chID, itmID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestTranscacheGetClonedNotFound(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				cache: map[string]*cachedItem{
					"otherItem": {},
				},
				clone: true,
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	if rcv, ok := tc.Get(chID, itmID); ok {
		t.Errorf("\nexpected to not find item, ok: %v", ok)
	} else if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

}

func TestTranscacheGetClonedNotClonable(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				lruIdx: list.New(),
				lruRefs: map[string]*list.Element{
					"testItemID": {},
				},
				cache: map[string]*cachedItem{
					"testItemID": {
						value: 3,
					},
				},
				clone: true,
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	if rcv, ok := tc.Get(chID, itmID); !ok {
		t.Error("Couldnt get cache value, ok: ", ok)
	} else if rcv != 3 {
		t.Fatalf("\nexpected <%+v>, \nreceived: <%+v>", 3, rcv)
	}
}

func TestTranscacheRewriteAllErr1(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	tc := TransCache{
		cache: map[string]*Cache{
			"testChID1": {
				offCollector: &OfflineCollector{
					writeLimit:      1000,
					file:            tmpFile,
					dumpInterval:    -1,
					rewriteInterval: -1,
					fldrPath:        "/tmp/notexistent",
				},
			},
		},
	}
	expErr := "error <lstat /tmp/notexistent: no such file or directory> walking path </tmp/notexistent>"
	if err := tc.RewriteAll(); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestTranscacheShutdownNil(t *testing.T) {
	tc := &TransCache{}
	exp := &TransCache{}
	tc.Shutdown()
	if !reflect.DeepEqual(exp, tc) {
		t.Errorf("Expected TransCache to not change, received <%+v>", tc)
	}
}

func TestTranscacheShutdownNoIntervalErr(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					writeLimit:      1000,
					dumpInterval:    -1,
					rewriteInterval: -1,
					logger:          &testLogger{log.New(&logBuf, "", 0)},
					file:            f,
				},
			},
		},
	}
	expBuf := "error getting file stats: stat /tmp/*default/file: file already closed"
	tc.Shutdown()
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)

	}
}

func TestTranscacheShutdownNilCollector(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			DefaultCacheInstance: {},
		},
	}
	tc.Shutdown()
	if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)

	}
}

func TestTranscacheShutdownIntervalWrite(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				offCollector: &OfflineCollector{
					dumpInterval:    5 * time.Second,
					rewriteInterval: -1,
					stopDump:        make(chan struct{}),
					dumpStopped:     make(chan struct{}),
					logger:          &testLogger{log.New(&logBuf, "", 0)},
					file:            f,
				},
			},
		},
	}
	go func() {
		tc.Shutdown()
		expBuf := "error getting file stats: stat /tmp/*default/file: file already closed"
		if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	for _, c := range tc.cache {
		<-c.offCollector.stopDump
		c.offCollector.dumpStopped <- struct{}{}
	}
	time.Sleep(50 * time.Millisecond)
}

func TestCloseFileSuccess(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-success-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	fileName := tmpFile.Name()

	err = closeFile(tmpFile)

	if err != nil {
		t.Error(err)
	}

	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Errorf("Expected file to be removed, but it still exists")
	}
}

func TestCloseFileErr(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	expErr := "error getting file stats: stat /tmp/*default/file: file already closed"
	if err := closeFile(f); err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestNewTransCacheWithOfflineCollector(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "", 1*time.Minute, 10*time.Second, 10*time.Second, 1000, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Error(err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
	expTc := NewTransCache(map[string]*CacheConfig{})

	expTc.cache[DefaultCacheInstance].onEvicted = tc.cache[DefaultCacheInstance].onEvicted
	expTc.cache[DefaultCacheInstance].lruIdx = tc.cache[DefaultCacheInstance].lruIdx
	expTc.cache[DefaultCacheInstance].ttlIdx = tc.cache[DefaultCacheInstance].ttlIdx
	expTc.cache[DefaultCacheInstance].offCollector = &OfflineCollector{
		dumpInterval:     10 * time.Second,
		stopDump:         tc.cache[DefaultCacheInstance].offCollector.stopDump,
		dumpStopped:      tc.cache[DefaultCacheInstance].offCollector.dumpStopped,
		rewriteInterval:  10 * time.Second,
		stopRewrite:      tc.cache[DefaultCacheInstance].offCollector.stopRewrite,
		rewriteStopped:   tc.cache[DefaultCacheInstance].offCollector.rewriteStopped,
		collection:       make(map[string]*CollectionEntity),
		fldrPath:         path + "/" + DefaultCacheInstance,
		collectSetEntity: true,
		writeLimit:       1000,
		file:             tc.cache[DefaultCacheInstance].offCollector.file,
		writer:           tc.cache[DefaultCacheInstance].offCollector.writer,
		encoder:          tc.cache[DefaultCacheInstance].offCollector.encoder,
		logger:           tc.cache[DefaultCacheInstance].offCollector.logger,
	}

	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("expected <%#v>, \nreceived <%#v>", expTc, tc)
	}
}

func TestNewTransCacheWithOfflineCollectorErr1(t *testing.T) {
	var logBuf bytes.Buffer
	_, err := NewTransCacheWithOfflineCollector("/tmp/doesntExist"+DefaultCacheInstance, "", 10*time.Second, 1*time.Minute, 10*time.Second, 1000, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	expErr := "stat /tmp/doesntExist*default: no such file or directory"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

}

func TestNewTransCacheWithOfflineCollectorErr2(t *testing.T) {
	path := "/root"
	var logBuf bytes.Buffer
	_, err := NewTransCacheWithOfflineCollector(path, "", 1*time.Minute, 10*time.Second, 10*time.Second, 1000, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	expErr := "mkdir /root/*default: permission denied"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

}

func TestNewTransCacheWithOfflineCollectorErr3(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/*default/tmpfile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("somethin not decodable by gob")
	defer func() {
		f.Close()
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	_, err = NewTransCacheWithOfflineCollector(path, "", 1*time.Minute, 10*time.Second, 10*time.Second, 1000, map[string]*CacheConfig{}, &testLogger{log.New(&logBuf, "", 0)})
	expErr := "failed to decode OfflineCacheEntity at </tmp/internal_db/*default/tmpfile>: unexpected EOF"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}

func TestTransCacheDumpAllDump0(t *testing.T) {
	tc := &TransCache{}
	expTc := &TransCache{}
	tc.DumpAll()
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
	expErr := "couldn't dump cache to file, *default offCollector is nil"
	if err := NewTransCache(map[string]*CacheConfig{}).DumpAll(); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestTransCacheDumpAllDumpErr1(t *testing.T) {
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				cache: map[string]*cachedItem{
					"Item1": {
						itemID:   "Item1",
						value:    "val",
						groupIDs: []string{"gr1"},
					},
				},
				offCollector: &OfflineCollector{
					dumpInterval: 1 * time.Second,
					writeLimit:   1,
					collection: map[string]*CollectionEntity{
						"Item1": {
							IsSet:  true,
							ItemID: "Item1",
						},
					},
					logger: &testLogger{log.New(&logBuf, "", 0)},
				},
			},
		},
	}
	expErr := "error getting file stat: invalid argument"
	if err := tc.DumpAll(); err == nil || err.Error() != expErr {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expErr, err)
	}
	if rcv := logBuf.String(); !strings.Contains(rcv, expErr) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}

func TestTransCacheDumpAllDumpErr2(t *testing.T) {
	var logBuf bytes.Buffer
	tc := &TransCache{
		cache: map[string]*Cache{
			DefaultCacheInstance: {
				cache: map[string]*cachedItem{
					"Item1": {
						itemID:   "Item1",
						value:    "val",
						groupIDs: []string{"gr1"},
					},
				},
				offCollector: &OfflineCollector{
					dumpInterval: 1 * time.Second,
					writeLimit:   1,
					collection: map[string]*CollectionEntity{
						"Item1": {
							IsSet:  false,
							ItemID: "Item1",
						},
					},
					logger: &testLogger{log.New(&logBuf, "", 0)},
				},
			},
		},
	}
	expErr := "error getting file stat: invalid argument"
	if err := tc.DumpAll(); err == nil || err.Error() != expErr {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expErr, err)

	}
	if rcv := logBuf.String(); !strings.Contains(rcv, expErr) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expErr, rcv)
	}
}

func TestTransCacheRewriteAllDump0(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	tc := &TransCache{}
	expTc := &TransCache{}
	if err := tc.RewriteAll(); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(expTc, tc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expTc, tc)
	}
	tc = NewTransCache(map[string]*CacheConfig{})
	tc.cache[DefaultCacheInstance].offCollector = &OfflineCollector{writeLimit: 1000,
		rewriteInterval: 0, file: tmpFile}
	expErr := "error <lstat : no such file or directory> walking path <>"
	if err := tc.RewriteAll(); err == nil || expErr != err.Error() {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestTransCacheAsyncRewriteEntitiesMinus1NoChanges(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	file, err := os.OpenFile(path+"/*default/file1", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	if err := encodeAndDump(&OfflineCacheEntity{IsSet: true,
		ItemID: "item1", Value: "val1", GroupIDs: []string{"gr1"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	file, err = os.OpenFile(path+"/*default/file2", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	if err := encodeAndDump(&OfflineCacheEntity{IsSet: true,
		ItemID: "item2", Value: "val2", GroupIDs: []string{"gr2"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	if files, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	} else if len(files) != 2 {
		t.Errorf("expected 2 files in <%v>, received <%v>", path+"/*default", len(files))
	}
	var logBuf bytes.Buffer
	offColl := NewOfflineCollector(path+"/*default", "", 1000, true, &testLogger{log.New(&logBuf, "", 0)}, 10000*time.Millisecond, -1)
	_, err = NewCacheFromFolder(offColl, -1, 0, false, true, nil)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(100 * time.Millisecond)
	expBuf := ""
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}

	if _, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	}

	f, err := os.Open(path + "/*default/0Rewrite0")
	if err != nil {
		t.Error(err)
	}
	dc := gob.NewDecoder(f)
	var rcv *OfflineCacheEntity
	if err := dc.Decode(&rcv); err != nil {
		t.Error(err)
	}
	exp := []*OfflineCacheEntity{
		{
			IsSet:    true,
			ItemID:   "item1",
			Value:    "val1",
			GroupIDs: []string{"gr1"},
		},
		{
			IsSet:    true,
			ItemID:   "item2",
			Value:    "val2",
			GroupIDs: []string{"gr2"},
		},
	}
	if !reflect.DeepEqual(exp[0], rcv) {
		if !reflect.DeepEqual(exp[1], rcv) {
			t.Errorf("expected <%+v>, received <%+v>", exp, rcv)
		}
	}
	rcv = nil
	if err := dc.Decode(&rcv); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp[0], rcv) {
		if !reflect.DeepEqual(exp[1], rcv) {
			t.Errorf("expected <%+v>, received <%+v>", exp, rcv)
		}
	}

}

func TestTransCacheAsyncRewriteEntitiesMinus1Changes(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	file, err := os.OpenFile(path+"/*default/file1", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	if err := encodeAndDump(&OfflineCacheEntity{IsSet: true,
		ItemID: "item1", Value: "val1", GroupIDs: []string{"gr1"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	file, err = os.OpenFile(path+"/*default/file2", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	if err := encodeAndDump(&OfflineCacheEntity{IsSet: false,
		ItemID: "item1"}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()

	if files, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	} else if len(files) != 2 {
		t.Errorf("expected 2 files in <%v>, received <%v>", path+"/*default", len(files))
	}
	var logBuf bytes.Buffer
	offColl := NewOfflineCollector(path+"/*default", "", -1, true, &testLogger{log.New(&logBuf, "", 0)}, 10000*time.Millisecond, -1)
	c, err := NewCacheFromFolder(offColl, -1, 0, false, true, nil)
	if err != nil {
		t.Error(err)
	}
	c.asyncRewriteEntities()
	time.Sleep(100 * time.Millisecond)
	expBuf := ""
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}

	if _, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	}

	var builder strings.Builder
	files, err := os.ReadDir(path + "/*default")
	if err != nil {
		t.Fatalf("Error reading directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(path+"/*default", file.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Error reading file %s: %v", file.Name(), err)
		}

		builder.Write(content)
	}
	combinedContent := builder.String()

	if combinedContent != "" {
		t.Errorf("Expected empty file, received <%s>", combinedContent)
	}
}

func TestTransCacheAsyncRewriteEntitiesIntervalChanges(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	file, err := os.OpenFile(path+"/*default/file1", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	if err := encodeAndDump(&OfflineCacheEntity{IsSet: true,
		ItemID: "item1", Value: "val1", GroupIDs: []string{"gr1"}}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()
	file, err = os.OpenFile(path+"/*default/file2", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	if err := encodeAndDump(&OfflineCacheEntity{IsSet: false,
		ItemID: "item1"}, encoder, writer); err != nil {
		t.Error(err)
	}
	file.Close()

	if files, err := os.ReadDir(path + "/*default"); err != nil {
		t.Error(err)
	} else if len(files) != 2 {
		t.Errorf("expected 2 files in <%v>, received <%v>", path+"/*default", len(files))
	}

	var logBuf bytes.Buffer
	offColl := NewOfflineCollector(path+"/*default", "", -1, true, &testLogger{log.New(&logBuf, "", 0)}, 1000*time.Millisecond, 10*time.Millisecond)
	c, err := NewCacheFromFolder(offColl, -1, 0, false, true, nil)
	if err != nil {
		t.Error(err)
	}

	go func() {
		c.asyncRewriteEntities()
		if rcv := logBuf.String(); rcv != "" {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
		}

		if _, err := os.ReadDir(path + "/*default"); err != nil {
			t.Error(err)
		}

		var builder strings.Builder
		files, err := os.ReadDir(path + "/*default")
		if err != nil {
			t.Errorf("Error reading directory: %v", err)
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			filePath := filepath.Join(path+"/*default", file.Name())
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Errorf("Error reading file %s: %v", file.Name(), err)
			}

			builder.Write(content)
		}
		combinedContent := builder.String()

		if combinedContent != "" {
			t.Errorf("Expected empty file, received <%s>", combinedContent)
		}
	}()
	time.Sleep(150 * time.Millisecond)
	c.offCollector.stopRewrite <- struct{}{}
	time.Sleep(50 * time.Millisecond)

}

func TestTranscacheShutdownErr(t *testing.T) {
	var logBuf bytes.Buffer
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.cache[DefaultCacheInstance].offCollector = &OfflineCollector{dumpInterval: 0,
		logger: &testLogger{log.New(&logBuf, "", 0)}}
	tc.Shutdown()
	expBuf := "error getting file stats: invalid argument"
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTransCacheBackupDumpFolderOK(t *testing.T) {
	path := "/tmp/db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	bkupPath := "/tmp/backups"
	if err := os.MkdirAll(bkupPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
		if err := os.RemoveAll(bkupPath); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "ignoredPath", 1*time.Minute, -1, -1, 1, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	tc.Set(DefaultCacheInstance, "newItem", "val", nil, true, "")
	time.Sleep(20 * time.Millisecond)
	if err := tc.BackupDumpFolder(bkupPath, false); err != nil {
		t.Error(err)
	}
	var bkupFldrPath string
	var found bool
	getFiles := func(dir string) ([]string, error) {
		var files []string
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && strings.Contains(info.Name(), "backups_") && !found {
				bkupFldrPath = filepath.Join(bkupPath, info.Name())
				found = true
			}
			if !info.IsDir() {
				relPath, _ := filepath.Rel(dir, path)
				files = append(files, relPath)
			}
			return nil
		})
		return files, err
	}
	org, err := getFiles(path)
	if err != nil {
		t.Error(err)
	}
	_, err = getFiles(bkupPath)
	if err != nil {
		t.Error(err)
	}
	bkup, err := getFiles(bkupFldrPath)
	if err != nil {
		t.Error(err)
	}
	if len(org) != len(bkup) {
		t.Errorf("Expected len <%v>, received <%v>", len(org), len(bkup))
	}
	for i := range org {
		if org[i] != bkup[i] {
			t.Errorf("Expected <%v>, received <%v>", org[i], bkup[i])
		}
	}
	sameFileContents := func(orgFile, bkupFile string) (bool, error) {
		of, err := os.ReadFile(orgFile)
		if err != nil {
			return false, err
		}
		bf, err := os.ReadFile(bkupFile)
		if err != nil {
			return false, err
		}

		if string(of) != string(bf) {
			return false, fmt.Errorf("expected \n<%s>, \nreceived\n<%s>", of, bf)
		}
		return true, nil
	}
	for _, of := range org {
		orgFilePath := filepath.Join(path, of)
		bkupFilePath := filepath.Join(bkupFldrPath, of)

		orgFile, err := os.Stat(orgFilePath)
		if err != nil {
			t.Error(err)
		}
		bkupFile, err := os.Stat(bkupFilePath)
		if err != nil {
			t.Error(err)
		}
		if orgFile.IsDir() {
			if !bkupFile.IsDir() {
				t.Fatalf("Expected path to not be a folder <%v>", bkupFile.Name())
			}
			continue
		}
		identical, err := sameFileContents(orgFilePath, bkupFilePath)
		if err != nil {
			t.Error(err)
		}
		if !identical {
			t.Errorf("File contents arent identical <%v>, <%v>", orgFilePath, bkupFilePath)
		}
	}
	if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}

func TestTransCacheBackupDumpFolderZip(t *testing.T) {
	path := "/tmp/db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	bkupPath := "/tmp/backups"
	if err := os.MkdirAll(bkupPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
		if err := os.RemoveAll(bkupPath); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "ignoredPath", 1*time.Minute, -1, -1, 1, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	tc.Set(DefaultCacheInstance, "newItem", "val", nil, true, "")
	time.Sleep(20 * time.Millisecond)
	if err := tc.BackupDumpFolder(bkupPath, true); err != nil {
		t.Fatal(err)
	}
	zippedBackups, err := filepath.Glob(filepath.Join(bkupPath, "*default", "backup_*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(zippedBackups) != 1 {
		t.Errorf("expected 1 zipped backup, received <%v>", zippedBackups)
	}
	if err := runCommand("unzip", []string{zippedBackups[0], "-d", bkupPath}); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll(zippedBackups[0]); err != nil {
		t.Errorf("Failed to delete temporary dir: %v", err)
	}

	getFiles := func(dir string) ([]string, error) {
		var files []string
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				relPath, _ := filepath.Rel(dir, path)
				files = append(files, relPath)
			}
			return nil
		})
		return files, err
	}
	org, err := getFiles(path)
	if err != nil {
		t.Error(err)
	}
	bkup, err := getFiles(bkupPath + "/" + DefaultCacheInstance)
	if err != nil {
		t.Error(err)
	}
	if len(org) != len(bkup) {
		t.Errorf("Expected len <%v>, received <%v>", len(org), len(bkup))
	}
	for i := range org {
		if strings.TrimPrefix(org[i], DefaultCacheInstance+"/") != bkup[i] {
			t.Errorf("Expected <%v>, received <%v>", org[i], bkup[i])
		}
	}
	sameFileContents := func(orgFile, bkupFile string) (bool, error) {
		of, err := os.ReadFile(orgFile)
		if err != nil {
			return false, err
		}
		bf, err := os.ReadFile(bkupFile)
		if err != nil {
			return false, err
		}

		if string(of) != string(bf) {
			return false, fmt.Errorf("expected \n<%s>, \nreceived\n<%s>", of, bf)
		}
		return true, nil
	}
	for _, of := range org {
		orgFilePath := filepath.Join(path, of)
		bkupFilePath := filepath.Join(bkupPath, of)

		orgFile, err := os.Stat(orgFilePath)
		if err != nil {
			t.Error(err)
		}
		bkupFile, err := os.Stat(bkupFilePath)
		if err != nil {
			t.Error(err)
		}
		if orgFile.IsDir() {
			if !bkupFile.IsDir() {
				t.Fatalf("Expected path to not be a folder <%v>", bkupFile.Name())
			}
			continue
		}
		identical, err := sameFileContents(orgFilePath, bkupFilePath)
		if err != nil {
			t.Error(err)
		}
		if !identical {
			t.Errorf("File contents arent identical <%v>, <%v>", orgFilePath, bkupFilePath)
		}
	}
	if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}
func TestTransCacheBackupDumpFolderErr(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	expErr := "Cache's offCollector is nil"
	if err := tc.BackupDumpFolder("", false); err == nil || expErr != err.Error() {
		t.Errorf("expected <%v>, received <%v>", expErr, err)
	}
}

func TestTransCacheBackupDumpFolderEmptyPath(t *testing.T) {
	path := "/tmp/db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	bkupPath := "/tmp/backups"
	if err := os.MkdirAll(bkupPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
		if err := os.RemoveAll(bkupPath); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "/tmp/backups", 1*time.Minute, -1, -1, 1, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	tc.Set(DefaultCacheInstance, "newItem", "val", nil, true, "")
	time.Sleep(20 * time.Millisecond)
	if err := tc.BackupDumpFolder("", false); err != nil {
		t.Error(err)
	}
	var bkupFldrPath string
	var found bool
	getFiles := func(dir string) ([]string, error) {
		var files []string
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && strings.Contains(info.Name(), "backups_") && !found {
				bkupFldrPath = filepath.Join(bkupPath, info.Name())
				found = true
			}
			if !info.IsDir() {
				relPath, _ := filepath.Rel(dir, path)
				files = append(files, relPath)
			}
			return nil
		})
		return files, err
	}
	org, err := getFiles(path)
	if err != nil {
		t.Error(err)
	}
	_, err = getFiles(bkupPath)
	if err != nil {
		t.Error(err)
	}
	bkup, err := getFiles(bkupFldrPath)
	if err != nil {
		t.Error(err)
	}
	if len(org) != len(bkup) {
		t.Errorf("Expected len <%v>, received <%v>", len(org), len(bkup))
	}
	for i := range org {
		if org[i] != bkup[i] {
			t.Errorf("Expected <%v>, received <%v>", org[i], bkup[i])
		}
	}
	sameFileContents := func(orgFile, bkupFile string) (bool, error) {
		of, err := os.ReadFile(orgFile)
		if err != nil {
			return false, err
		}
		bf, err := os.ReadFile(bkupFile)
		if err != nil {
			return false, err
		}

		if string(of) != string(bf) {
			return false, fmt.Errorf("expected \n<%s>, \nreceived\n<%s>", of, bf)
		}
		return true, nil
	}
	for _, of := range org {
		orgFilePath := filepath.Join(path, of)
		bkupFilePath := filepath.Join(bkupFldrPath, of)

		orgFile, err := os.Stat(orgFilePath)
		if err != nil {
			t.Error(err)
		}
		bkupFile, err := os.Stat(bkupFilePath)
		if err != nil {
			t.Error(err)
		}
		if orgFile.IsDir() {
			if !bkupFile.IsDir() {
				t.Fatalf("Expected path to not be a folder <%v>", bkupFile.Name())
			}
			continue
		}
		identical, err := sameFileContents(orgFilePath, bkupFilePath)
		if err != nil {
			t.Error(err)
		}
		if !identical {
			t.Errorf("File contents arent identical <%v>, <%v>", orgFilePath, bkupFilePath)
		}
	}
	if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

}

func TestTransCacheBackupDumpFolderErr2(t *testing.T) {
	path := "/tmp/db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	bkupPath := "/tmp/backups"
	if err := os.MkdirAll(bkupPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
		if err := os.RemoveAll(bkupPath); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "/tmp/backups", 1*time.Minute, -1, -1, 1, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	tc.Set(DefaultCacheInstance, "newItem", "val", nil, true, "")
	time.Sleep(20 * time.Millisecond)
	expErr := `command <sh> failed with error <exit status 15>, Output <zip I/O error: No such file or directory
zip error: Could not create output file (.../*default/backup_*default`
	if err := tc.BackupDumpFolder("...", true); err == nil ||
		!strings.HasPrefix(err.Error(), expErr) {
		t.Errorf("expected error <%v>, \nreceived <%v>", expErr, err)
	}
}

func TestTransCacheBackupDumpFolderErr3(t *testing.T) {
	path := "/tmp/db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
		if err := os.RemoveAll("-"); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "/tmp/backups", 1*time.Minute, -1, -1, 1, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	tc.Set(DefaultCacheInstance, "newItem", "val", nil, true, "")
	time.Sleep(20 * time.Millisecond)
	expErr := `command <cp> failed with error <exit status 1>, Output <cp: invalid option -- '/'
Try 'cp --help' for more information.
>`
	if err := tc.BackupDumpFolder("-", false); err == nil || err.Error() != expErr {
		t.Errorf("expected error <%v>, \nreceived <%v>", expErr, err)
	}
}

func TestTransCacheBackupDumpFolderErr4(t *testing.T) {
	path := "/tmp/db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
		if err := os.RemoveAll("-"); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	tc, err := NewTransCacheWithOfflineCollector(path, "/tmp/backups", 1*time.Minute, -1, -1, 1, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	tc.Set(DefaultCacheInstance, "newItem", "val", nil, true, "")
	time.Sleep(20 * time.Millisecond)
	expErr := `: permission denied`
	if err := tc.BackupDumpFolder("/", false); err == nil ||
		!strings.HasSuffix(err.Error(), expErr) {
		t.Errorf("expected error <%v>, \nreceived <%v>", expErr, err)
	}
}

func TestRunCommandErr(t *testing.T) {
	expErr := `command <bad command> failed with error <exec: "bad command": executable file not found in $PATH>, Output <>`
	if err := runCommand("bad command", []string{}); err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received error <%v>", expErr, err)
	}
}

func TestNewTransCacheWithOfflineCollectorWriteLimitErr(t *testing.T) {
	var logBuf bytes.Buffer
	expErr := "writeLimit has to be bigger than 0. Current writeLimit <0>"
	if _, err := NewTransCacheWithOfflineCollector("", "", 0, 0, 0, 0, map[string]*CacheConfig{}, nil); err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received <%v>", expErr, err)
	} else if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

}

func TestNewTransCacheWithOfflineCollectorTimeoutErr(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var logBuf bytes.Buffer
	expErr := `Building TransCache from </tmp/internal_db> timed out after <0s>`
	if _, err := NewTransCacheWithOfflineCollector(path, "", 0, 10*time.Second, 10*time.Second, 1000, map[string]*CacheConfig{},
		&testLogger{log.New(&logBuf, "", 0)}); err == nil || expErr != err.Error() {
		t.Errorf("expected error <%v>, received error <%v>", expErr, err)
	} else if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}

// BenchmarkSet            	 3000000	       469 ns/op
func BenchmarkSet(b *testing.B) {
	cacheItems := [][]string{
		{"aaa_", "1", "1"},
		{"aaa_", "2", "1"},
		{"aaa_", "3", "1"},
		{"aaa_", "4", "1"},
		{"aaa_", "5", "1"},
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(cacheItems)-1 // so we can have random index
	tc := NewTransCache(map[string]*CacheConfig{})
	for n := 0; n < b.N; n++ {
		ci := cacheItems[rand.Intn(max-min)+min]
		tc.Set(ci[0], ci[1], ci[2], nil, false, "")
	}
}

// BenchmarkSetWithGroups  	 3000000	       591 ns/op
func BenchmarkSetWithGroups(b *testing.B) {
	cacheItems := [][]string{
		{"aaa_", "1", "1"},
		{"aaa_", "2", "1"},
		{"aaa_", "3", "1"},
		{"aaa_", "4", "1"},
		{"aaa_", "5", "1"},
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(cacheItems)-1 // so we can have random index
	tc := NewTransCache(map[string]*CacheConfig{})
	for n := 0; n < b.N; n++ {
		ci := cacheItems[rand.Intn(max-min)+min]
		tc.Set(ci[0], ci[1], ci[2], []string{"grp1", "grp2"}, false, "")
	}
}

// BenchmarkGet            	10000000	       163 ns/op
func BenchmarkGet(b *testing.B) {
	cacheItems := [][]string{
		{"aaa_", "1", "1"},
		{"aaa_", "2", "1"},
		{"aaa_", "3", "1"},
		{"aaa_", "4", "1"},
		{"aaa_", "5", "1"},
	}
	tc := NewTransCache(map[string]*CacheConfig{})
	for _, ci := range cacheItems {
		tc.Set(ci[0], ci[1], ci[2], nil, false, "")
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(cacheItems)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		tc.Get("aaa_", cacheItems[rand.Intn(max-min)+min][0])
	}
}
