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

func TestTransactionSetWithOffCollector(t *testing.T) {
	var logBuf bytes.Buffer
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.offCollector = &OfflineCollector{
		dumpInterval: time.Second,
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {},
		},
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		logger:     &testLogger{log.New(&logBuf, "", 0)},
	}

	tc.Set("*default", "CacheID1", "CacheValue1", []string{"CacheGroup1"}, true, "")
	if t1, ok := tc.Get("*default", "CacheID1"); !ok || t1 != "CacheValue1" {
		t.Errorf("Expected <CacheValue1>, received <%+v>", t1)
	}
	if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
	expOC := &OfflineCollector{
		dumpInterval: time.Second,
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {
				"CacheID1": {
					IsSet:   true,
					CacheID: "CacheID1",
				},
			},
		},
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		logger:     tc.offCollector.logger,
	}
	if !reflect.DeepEqual(expOC, tc.offCollector) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC, tc.offCollector)
	}
}

func TestTransactionSetWithOffCollectorErr(t *testing.T) {
	var logBuf bytes.Buffer
	tc := NewTransCache(map[string]*CacheConfig{})
	f, err := os.OpenFile("/tmp/tmpfile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	f.Close()
	defer func() {
		if err := os.RemoveAll("/tmp/tmpfile"); err != nil {
			t.Errorf("Failed to delete temporary file: %v", err)
		}
	}()
	tc.offCollector = &OfflineCollector{
		dumpInterval: -1,
		writeLimit:   1,
		files: map[string]*os.File{
			"*default": f,
		},
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		logger:     &testLogger{log.New(&logBuf, "", 0)},
	}

	tc.Set("*default", "CacheID1", "CacheValue1", []string{"CacheGroup1"}, true, "")
	if t1, ok := tc.Get("*default", "CacheID1"); !ok || t1 != "CacheValue1" {
		t.Errorf("Expected <CacheValue1>, received <%+v>", t1)
	}
	expErr := "error getting file stat: stat /tmp/tmpfile: file already closed"
	if rcv := logBuf.String(); !strings.Contains(rcv, expErr) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expErr, rcv)
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
		"dst_": &CacheConfig{MaxItems: -1},
		"rpf_": &CacheConfig{MaxItems: -1}})
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
		"part1": &CacheConfig{MaxItems: -1},
		"part2": &CacheConfig{MaxItems: -1}})
	testCIs := []*cachedItem{
		&cachedItem{itemID: "_1_", value: "one"},
		&cachedItem{itemID: "_2_", value: "two", groupIDs: []string{"grp1"}},
		&cachedItem{itemID: "_3_", value: "three", groupIDs: []string{"grp1", "grp2"}},
		&cachedItem{itemID: "_4_", value: "four", groupIDs: []string{"grp1", "grp2", "grp3"}},
		&cachedItem{itemID: "_5_", value: "five", groupIDs: []string{"grp4"}},
	}
	for _, ci := range testCIs {
		tc.Set("part1", ci.itemID, ci.value, ci.groupIDs, true, "")
	}
	for _, ci := range testCIs[:4] {
		tc.Set("part2", ci.itemID, ci.value, ci.groupIDs, true, "")
	}
	eCs := map[string]*CacheStats{
		"part1": &CacheStats{Items: 5, Groups: 4},
		"part2": &CacheStats{Items: 4, Groups: 3},
	}
	if cs := tc.GetCacheStats(nil); reflect.DeepEqual(eCs, cs) {
		t.Errorf("expecting: %+v, received: %+v", eCs, cs)
	}
}

// Try concurrent read/write of the cache
func TestCacheConcurrent(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{
		"dst_": &CacheConfig{MaxItems: -1},
		"rpf_": &CacheConfig{MaxItems: -1}})
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

func (tID *TenantID) Clone() (interface{}, error) {
	tClone := new(TenantID)
	*tClone = *tID
	return tClone, nil
}

func TestGetClone(t *testing.T) {
	tc := NewTransCache(map[string]*CacheConfig{})
	a := &TenantID{Tenant: "cgrates.org", ID: "ID#1"}
	tc.Set("t11_", "mm", a, nil, true, "")
	if t1, ok := tc.Get("t11_", "mm"); !ok {
		t.Error("Error setting cache: ", ok, t1)
	}
	if x, err := tc.GetCloned("t11_", "mm"); err != nil {
		t.Error(err)
	} else {
		tcCloned := x.(*TenantID)
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
	tc := NewTransCache(map[string]*CacheConfig{})
	tc.Set("t11_", "mm", nil, nil, true, "")
	if x, err := tc.GetCloned("t11_", "mm"); err != nil {
		t.Error(err)
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
	if err := os.MkdirAll("/tmp/internal_db/testChID1", 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("/tmp/internal_db/testChID2", 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("/tmp/internal_db")
	var logBuf bytes.Buffer
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
		offCollector: &OfflineCollector{
			folderPath: "/tmp/internal_db",
			files:      make(map[string]*os.File),
			writers:    make(map[string]*bufio.Writer),
			encoders:   make(map[string]*gob.Encoder),
			logger:     &testLogger{log.New(&logBuf, "", 0)},
			setCollMux: map[string]*sync.RWMutex{"testChID1": new(sync.RWMutex),
				"testChID2": new(sync.RWMutex)},
			setColl: map[string]map[string]*OfflineCacheEntity{
				"testChID1": {
					"item1": {
						IsSet:   true,
						CacheID: "item1",
					},
					"item2": {
						IsSet:   true,
						CacheID: "item2",
					},
				},
				"testChID2": {
					"item1": {
						IsSet:   true,
						CacheID: "item1",
					},
					"item2": {
						IsSet:   true,
						CacheID: "item2",
					},
					"item3": {
						IsSet:   true,
						CacheID: "item3",
					},
				},
			},
			remColl: map[string][]string{"testChID1": {"item3"}},
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
	expOC := &OfflineCollector{
		setColl: map[string]map[string]*OfflineCacheEntity{
			"testChID1": {},
			"testChID2": {},
		},
		remColl: map[string][]string{"testChID1": {}, "testChID2": {}},
	}
	if !reflect.DeepEqual(expOC.setColl, tc.offCollector.setColl) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.setColl, tc.offCollector.setColl)
	} else if !reflect.DeepEqual(expOC.remColl, tc.offCollector.remColl) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.remColl, tc.offCollector.remColl)
	} else if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
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
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	experr := ErrNotFound
	rcv, err := tc.GetCloned(chID, itmID)

	if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

	if err == nil || err != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
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
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	experr := ErrNotClonable
	rcv, err := tc.GetCloned(chID, itmID)

	if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

	if err == nil || err != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

type clonerMock struct {
	testcase string
}

func (cM *clonerMock) Clone() (interface{}, error) {
	switch cM.testcase {
	case "clone error":
		err := fmt.Errorf("clone mock error")
		return nil, err
	}
	return nil, nil
}
func TestTranscacheGetClonedCloneError(t *testing.T) {
	cloner := &clonerMock{
		testcase: "clone error",
	}
	tc := &TransCache{
		cache: map[string]*Cache{
			"testChID": {
				lruIdx: list.New(),
				lruRefs: map[string]*list.Element{
					"testItemID": {},
				},
				cache: map[string]*cachedItem{
					"testItemID": {
						value: cloner,
					},
				},
			},
		},
	}
	chID := "testChID"
	itmID := "testItemID"

	experr := "clone mock error"
	rcv, err := tc.GetCloned(chID, itmID)

	if rcv != nil {
		t.Fatalf("\nexpected nil, \nreceived: <%+v>", rcv)
	}

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestTranscacheRewrite(t *testing.T) {
	var logBuf bytes.Buffer
	tc := TransCache{
		offCollector: &OfflineCollector{
			folderPath: "/tmp/notexistent",
			logger:     &testLogger{log.New(&logBuf, "", 0)},
		},
	}
	bufExpect := "Failed to read dir </tmp/notexistent> error: open /tmp/notexistent: no such file or directory"
	if err := tc.Rewrite(); err != nil {
		t.Error(err)
	} else if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestTranscacheRewriteErr(t *testing.T) {
	tc := TransCache{}
	expErr := "InternalDB dump not activated"
	if err := tc.Rewrite(); err.Error() != expErr {
		t.Errorf("Expected error <%v>, received <%v>", expErr, err)
	}

}

func TestTranscacheShutdownShutdownNil(t *testing.T) {
	tc := &TransCache{}
	exp := &TransCache{}
	tc.Shutdown()
	if !reflect.DeepEqual(exp, tc) {
		t.Errorf("Expected TransCache to not change, received <%+v>", tc)
	}
}
func TestTranscacheShutdownShutdownNoIntervalErr(t *testing.T) {
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
		offCollector: &OfflineCollector{
			dumpInterval:    -1,
			rewriteInterval: -1,
			files: map[string]*os.File{
				"*default": f,
			},
			logger: &testLogger{log.New(&logBuf, "", 0)},
		},
	}
	expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
	tc.Shutdown()
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTranscacheShutdownShutdownRewrite(t *testing.T) {
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
		offCollector: &OfflineCollector{
			dumpInterval:    -1,
			rewriteInterval: -2,
			files: map[string]*os.File{
				"*default": f,
			},
			logger: &testLogger{log.New(&logBuf, "", 0)},
		},
	}
	expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
	tc.Shutdown()
	if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
	}
}

func TestTranscacheShutdownShutdownIntervalRewrite(t *testing.T) {
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
		offCollector: &OfflineCollector{
			dumpInterval:    -1,
			rewriteInterval: 5 * time.Second,
			stopRewrite:     make(chan struct{}),
			rewriteStopped:  make(chan struct{}),
			files: map[string]*os.File{
				"*default": f,
			},
			logger: &testLogger{log.New(&logBuf, "", 0)},
		},
	}
	go func() {
		tc.Shutdown()
		expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
		if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	<-tc.offCollector.stopRewrite
	tc.offCollector.rewriteStopped <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTranscacheShutdownShutdownIntervalWrite(t *testing.T) {
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
		offCollector: &OfflineCollector{
			dumpInterval:    5 * time.Second,
			rewriteInterval: -1,
			stopWriting:     make(chan struct{}),
			writeStopped:    make(chan struct{}),
			files: map[string]*os.File{
				"*default": f,
			},
			logger: &testLogger{log.New(&logBuf, "", 0)},
		},
	}
	go func() {
		tc.Shutdown()
		expBuf := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
		if rcv := logBuf.String(); !strings.Contains(rcv, expBuf) {
			t.Errorf("Expected <%+v>, \nReceived <%+v>", expBuf, rcv)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	<-tc.offCollector.stopWriting
	tc.offCollector.writeStopped <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTransCacheReadAllOK(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpOC := &OfflineCollector{
		files:    map[string]*os.File{"*default": tmpFile},
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	tmpOC.writers["*default"] = bufio.NewWriter(tmpOC.files["*default"])
	tmpOC.encoders["*default"] = gob.NewEncoder(tmpOC.writers["*default"])
	if err := tmpOC.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			folderPath:   "/tmp/internal_db/",
			dumpInterval: 5 * time.Second,
			writeStopped: make(chan struct{}),
			stopWriting:  make(chan struct{}),
			files:        map[string]*os.File{"*default": tmpFile},
			setCollMux:   make(map[string]*sync.RWMutex),
			writers:      make(map[string]*bufio.Writer),
			encoders:     make(map[string]*gob.Encoder),
		},
	}
	go func() {
		if err := tc.ReadAll(); err != nil {
			t.Error(err)
		}
		exp := &cachedItem{
			itemID:     "CacheID",
			value:      "sampleValue",
			expiryTime: time.Time{},
			groupIDs:   []string{"CacheGroup1"},
		}
		if !reflect.DeepEqual(exp, tc.cache["*default"].cache["CacheID"]) {
			t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache["*default"].cache["CacheID"])
		}
	}()
	time.Sleep(50 * time.Millisecond)
	tc.offCollector.stopWriting <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTransCacheReadAllWithDumpInterval(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpOC := &OfflineCollector{
		files:    map[string]*os.File{"*default": tmpFile},
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	tmpOC.writers["*default"] = bufio.NewWriter(tmpOC.files["*default"])
	tmpOC.encoders["*default"] = gob.NewEncoder(tmpOC.writers["*default"])
	if err := tmpOC.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			folderPath:   "/tmp/internal_db/",
			dumpInterval: 100 * time.Millisecond,
			writeStopped: make(chan struct{}),
			stopWriting:  make(chan struct{}),
			files:        map[string]*os.File{"*default": tmpFile},
			setCollMux:   make(map[string]*sync.RWMutex),
			writers:      make(map[string]*bufio.Writer),
			encoders:     make(map[string]*gob.Encoder),
		},
	}
	go func() {
		if err := tc.ReadAll(); err != nil {
			t.Error(err)
		}
		exp := &cachedItem{
			itemID:     "CacheID",
			value:      "sampleValue",
			expiryTime: time.Time{},
			groupIDs:   []string{"CacheGroup1"},
		}
		if !reflect.DeepEqual(exp, tc.cache["*default"].cache["CacheID"]) {
			t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache["*default"].cache["CacheID"])
		}
	}()
	time.Sleep(150 * time.Millisecond)
	tc.offCollector.stopWriting <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTransCacheReadAllMinus1DumpInterval(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpOC := &OfflineCollector{
		files:    map[string]*os.File{"*default": tmpFile},
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	tmpOC.writers["*default"] = bufio.NewWriter(tmpOC.files["*default"])
	tmpOC.encoders["*default"] = gob.NewEncoder(tmpOC.writers["*default"])
	if err := tmpOC.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			dumpInterval: -1,
			folderPath:   "/tmp/internal_db/",
			writeStopped: make(chan struct{}),
			files:        map[string]*os.File{"*default": tmpFile},
			setCollMux:   make(map[string]*sync.RWMutex),
			writers:      make(map[string]*bufio.Writer),
			encoders:     make(map[string]*gob.Encoder),
		},
	}
	if err := tc.ReadAll(); err != nil {
		t.Error(err)
	}
	exp := &cachedItem{
		itemID:     "CacheID",
		value:      "sampleValue",
		expiryTime: time.Time{},
		groupIDs:   []string{"CacheGroup1"},
	}
	if !reflect.DeepEqual(exp, tc.cache["*default"].cache["CacheID"]) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache["*default"].cache["CacheID"])
	}
}

func TestTransCacheReadAll0DumpInterval(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpOC := &OfflineCollector{
		files:    map[string]*os.File{"*default": tmpFile},
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	tmpOC.writers["*default"] = bufio.NewWriter(tmpOC.files["*default"])
	tmpOC.encoders["*default"] = gob.NewEncoder(tmpOC.writers["*default"])
	if err := tmpOC.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			dumpInterval: 0,
			folderPath:   "/tmp/internal_db/",
			writeStopped: make(chan struct{}),
			files:        map[string]*os.File{"*default": tmpFile},
			setCollMux:   make(map[string]*sync.RWMutex),
			writers:      make(map[string]*bufio.Writer),
			encoders:     make(map[string]*gob.Encoder),
		},
	}
	if err := tc.ReadAll(); err != nil {
		t.Error(err)
	}
	exp := &cachedItem{
		itemID:     "CacheID",
		value:      "sampleValue",
		expiryTime: time.Time{},
		groupIDs:   []string{"CacheGroup1"},
	}
	if !reflect.DeepEqual(exp, tc.cache["*default"].cache["CacheID"]) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache["*default"].cache["CacheID"])
	}
}

func TestTransCacheReadAllRewriteMinus1(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpOC := &OfflineCollector{
		files:    map[string]*os.File{"*default": tmpFile},
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	tmpOC.writers["*default"] = bufio.NewWriter(tmpOC.files["*default"])
	tmpOC.encoders["*default"] = gob.NewEncoder(tmpOC.writers["*default"])
	if err := tmpOC.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			folderPath:      "/tmp/internal_db/",
			dumpInterval:    -1,
			rewriteInterval: -1,
			writeStopped:    make(chan struct{}),
			stopWriting:     make(chan struct{}),
			files:           map[string]*os.File{"*default": tmpFile},
			setCollMux:      make(map[string]*sync.RWMutex),
			writers:         make(map[string]*bufio.Writer),
			encoders:        make(map[string]*gob.Encoder),
		},
	}
	if err := tc.ReadAll(); err != nil {
		t.Error(err)
	}
	exp := &cachedItem{
		itemID:     "CacheID",
		value:      "sampleValue",
		expiryTime: time.Time{},
		groupIDs:   []string{"CacheGroup1"},
	}
	if !reflect.DeepEqual(exp, tc.cache["*default"].cache["CacheID"]) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache["*default"].cache["CacheID"])
	}
}

func TestTransCacheReadAllWithRewriteInterval(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", "testfile-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpOC := &OfflineCollector{
		files:    map[string]*os.File{"*default": tmpFile},
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	tmpOC.writers["*default"] = bufio.NewWriter(tmpOC.files["*default"])
	tmpOC.encoders["*default"] = gob.NewEncoder(tmpOC.writers["*default"])
	if err := tmpOC.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			folderPath:      "/tmp/internal_db/",
			rewriteInterval: 100 * time.Millisecond,
			writeStopped:    make(chan struct{}),
			stopWriting:     make(chan struct{}),
			rewriteStopped:  make(chan struct{}),
			stopRewrite:     make(chan struct{}),
			files:           map[string]*os.File{"*default": tmpFile},
			setCollMux:      make(map[string]*sync.RWMutex),
			writers:         make(map[string]*bufio.Writer),
			encoders:        make(map[string]*gob.Encoder),
		},
	}
	go func() {
		if err := tc.ReadAll(); err != nil {
			t.Error(err)
		}
		exp := &cachedItem{
			itemID:     "CacheID",
			value:      "sampleValue",
			expiryTime: time.Time{},
			groupIDs:   []string{"CacheGroup1"},
		}
		if !reflect.DeepEqual(exp, tc.cache["*default"].cache["CacheID"]) {
			t.Errorf("Expected <%+v>, Received <%+v>", exp, tc.cache["*default"].cache["CacheID"])
		}
	}()
	time.Sleep(150 * time.Millisecond)
	tc.offCollector.stopRewrite <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestTransCacheReadAllErr1(t *testing.T) {
	tc := &TransCache{
		offCollector: &OfflineCollector{
			folderPath: "/var/noAccess",
		},
	}
	expErr := "mkdir /var/noAccess: permission denied"
	if err := tc.ReadAll(); err.Error() != expErr {
		t.Errorf("Expected error <%v>, received <%v>", expErr, err)
	}
}

func TestTransCacheReadAllErr2(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/*default/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("some undecodable string")
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tc := &TransCache{
		cfg: map[string]*CacheConfig{
			"*default": {},
		},
		cache: make(map[string]*Cache),
		offCollector: &OfflineCollector{
			dumpInterval: 0,
			folderPath:   "/tmp/internal_db/",
			writeStopped: make(chan struct{}),
			files:        map[string]*os.File{"*default": f},
			setCollMux:   make(map[string]*sync.RWMutex),
			writers:      make(map[string]*bufio.Writer),
			encoders:     make(map[string]*gob.Encoder),
		},
	}
	expErr := "failed to decode OfflineCacheEntity at </tmp/internal_db/*default/file>: unexpected EOF"
	if err := tc.ReadAll(); err.Error() != expErr {
		t.Errorf("Expected error <%v>, received <%v>", expErr, err)
	}
}

func TestTransCacheWriteAllOK(t *testing.T) {
	encBuf := &bytes.Buffer{}
	writeBuf := &bytes.Buffer{}
	encBuf2 := &bytes.Buffer{}
	writeBuf2 := &bytes.Buffer{}
	tc := &TransCache{
		cache: map[string]*Cache{
			"*default":  NewCache(-1, -1, false, nil),
			"*sessions": NewCache(-1, -1, false, nil),
		},
		offCollector: &OfflineCollector{
			setColl: map[string]map[string]*OfflineCacheEntity{
				"*default": {
					"TestCHID1": &OfflineCacheEntity{
						IsSet:   true,
						CacheID: "CacheID",
					},
				},
			},
			remColl: map[string][]string{
				"*sessions": {"ToDeleteCHID1"},
			},
			setCollMux: map[string]*sync.RWMutex{"*default": {}, "*sessions": {}},
			writeLimit: -1,
			encoders:   map[string]*gob.Encoder{"*default": gob.NewEncoder(encBuf), "*sessions": gob.NewEncoder(encBuf2)},
			writers:    map[string]*bufio.Writer{"*default": bufio.NewWriter(writeBuf), "*sessions": bufio.NewWriter(writeBuf2)},
		},
	}
	tc.cache["*default"].cache["TestCHID1"] = &cachedItem{value: "sampleValue", groupIDs: []string{"CacheGroup1"}}
	if err := tc.WriteAll(); err != nil {
		t.Error(err)
	}
	bufExpect := "TestCHID1"
	if rcv := encBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
	bufExpect = "ToDeleteCHID1"
	if rcv := encBuf2.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
	bufExpect = ""
	if rcv := writeBuf.String(); rcv != "" {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
	if rcv := writeBuf2.String(); rcv != "" {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestTransCacheWriteAllErr1(t *testing.T) {
	tc := &TransCache{}
	expErr := "InternalDB dump not activated"
	if err := tc.WriteAll(); err.Error() != expErr {
		t.Errorf("Expected error <%v>, received <%v>", expErr, err)
	}
}

func TestTransCacheWriteAllErr2(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{"*default": {}},
		offCollector: &OfflineCollector{
			setColl: map[string]map[string]*OfflineCacheEntity{
				"*default": {
					"CacheID": &OfflineCacheEntity{
						IsSet:    true,
						CacheID:  "CacheID",
						Value:    "sampleValue",
						GroupIDs: []string{"CacheGroup1"},
					},
				},
			},
			remColl: map[string][]string{
				"*default": {"ToDeleteCacheID1"},
			},
			setCollMux: map[string]*sync.RWMutex{"*default": {}},
		},
	}
	expErr := "error getting file stat: invalid argument"
	if err := tc.WriteAll(); err.Error() != expErr {
		t.Errorf("Expected error <%v>, received <%v>", expErr, err)
	}
}

func TestTransCacheWriteAllErr3(t *testing.T) {
	tc := &TransCache{
		cache: map[string]*Cache{
			"*default":  NewCache(-1, -1, false, nil),
			"*sessions": NewCache(-1, -1, false, nil),
		},
		offCollector: &OfflineCollector{
			setColl: map[string]map[string]*OfflineCacheEntity{
				"*default": {
					"CacheID": &OfflineCacheEntity{
						IsSet:   true,
						CacheID: "CacheID",
					},
				},
			},
			remColl: map[string][]string{
				"*sessions": {"ToDeleteCacheID1"},
			},
			setCollMux: map[string]*sync.RWMutex{"*default": {}, "*sessions": {}},
		},
	}
	tc.cache["*default"].cache["CacheID"] = &cachedItem{value: "sampleValue", groupIDs: []string{"CacheGroup1"}}
	expErr := "error getting file stat: invalid argument"
	if err := tc.WriteAll(); err.Error() != expErr {
		t.Errorf("Expected error <%v>, received <%v>", expErr, err)
	}
}

// BenchmarkSet            	 3000000	       469 ns/op
func BenchmarkSet(b *testing.B) {
	cacheItems := [][]string{
		[]string{"aaa_", "1", "1"},
		[]string{"aaa_", "2", "1"},
		[]string{"aaa_", "3", "1"},
		[]string{"aaa_", "4", "1"},
		[]string{"aaa_", "5", "1"},
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
		[]string{"aaa_", "1", "1"},
		[]string{"aaa_", "2", "1"},
		[]string{"aaa_", "3", "1"},
		[]string{"aaa_", "4", "1"},
		[]string{"aaa_", "5", "1"},
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
		[]string{"aaa_", "1", "1"},
		[]string{"aaa_", "2", "1"},
		[]string{"aaa_", "3", "1"},
		[]string{"aaa_", "4", "1"},
		[]string{"aaa_", "5", "1"},
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
