/*
ltcache.go is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

A LRU cache with TTL capabilities.

*/

package ltcache

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testCIs = []*cachedItem{
	{itemID: "_1_", value: "one"},
	{itemID: "_2_", value: "two", groupIDs: []string{"grp1"}},
	{itemID: "_3_", value: "three", groupIDs: []string{"grp1", "grp2"}},
	{itemID: "_4_", value: "four", groupIDs: []string{"grp1", "grp2", "grp3"}},
	{itemID: "_5_", value: "five", groupIDs: []string{"grp4"}},
}
var lastEvicted string

func TestSetGetRemNoIndexes(t *testing.T) {
	cache := NewCache(UnlimitedCaching, 0, false, false,
		[]func(itmID string, value any){func(itmID string, v interface{}) { lastEvicted = itmID }})
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, ci.groupIDs)
	}
	if len(cache.cache) != 5 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if len(cache.groups) != 4 {
		t.Errorf("Wrong intems in groups: %+v", cache.groups)
	} else if len(cache.groups["grp1"]) != 3 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp1"])
	} else if len(cache.groups["grp2"]) != 2 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp2"])
	} else if len(cache.groups["grp3"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp3"])
	} else if len(cache.groups["grp4"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp4"])
	}
	if cache.lruIdx.Len() != 0 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != 0 {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != 0 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != 0 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
	if itmIDs := cache.GetItemIDs(""); len(itmIDs) != 5 {
		t.Errorf("received: %+v", itmIDs)
	}
	if itmIDs := cache.GetItemIDs("_"); len(itmIDs) != 5 {
		t.Errorf("received: %+v", itmIDs)
	}
	if itmIDs := cache.GetItemIDs("_1"); len(itmIDs) != 1 {
		t.Errorf("received: %+v", itmIDs)
	}
	if itmIDs := cache.GetItemIDs("_1_"); len(itmIDs) != 1 {
		t.Errorf("received: %+v", itmIDs)
	}
	if itmIDs := cache.GetItemIDs("_1__"); len(itmIDs) != 0 {
		t.Errorf("received: %+v", itmIDs)
	}
	if val, has := cache.Get("_2_"); !has {
		t.Error("item not in cache")
	} else if val.(string) != "two" {
		t.Errorf("wrong item value: %v", val)
	}
	if len(cache.cache) != 5 {
		t.Errorf("wrong keys: %+v", cache.cache)
	}
	eCs := &CacheStats{Items: 5, Groups: 4}
	if cs := cache.GetCacheStats(); !reflect.DeepEqual(eCs, cs) {
		t.Errorf("expecting: %+v, received: %+v", eCs, cs)
	}
	cache.Set("_2_", "twice", []string{"grp21"})
	if val, has := cache.Get("_2_"); !has {
		t.Error("item not in cache")
	} else if val.(string) != "twice" {
		t.Errorf("wrong item value: %v", val)
	}
	if len(cache.groups) != 5 {
		t.Errorf("Wrong intems in groups: %+v", cache.groups)
	} else if len(cache.groups["grp1"]) != 2 { // one gone through set
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp1"])
	} else if len(cache.groups["grp21"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp21"])
	}
	if lastEvicted != "" {
		t.Error("lastEvicted var should be empty")
	}
	cache.Remove("_2_")
	if len(cache.groups) != 4 {
		t.Errorf("Wrong intems in groups: %+v", cache.groups)
	} else if len(cache.groups["grp1"]) != 2 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp1"])
	} else if len(cache.groups["grp21"]) != 0 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp21"])
	}
	if lastEvicted != "_2_" { // onEvicted should populate this var
		t.Error("lastEvicted var should be 2")
	}
	if _, has := cache.Get("_2_"); has {
		t.Error("item still in cache")
	}
	if len(cache.cache) != 4 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.Len() != 4 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	cache.Remove("_3_")
	if len(cache.groups) != 4 {
		t.Errorf("Wrong intems in groups: %+v", cache.groups)
	} else if len(cache.groups["grp1"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp1"])
	} else if len(cache.groups["grp2"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp2"])
	} else if len(cache.groups["grp3"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp3"])
	} else if len(cache.groups["grp4"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp4"])
	}
	cache.RemoveGroup("nonexistent")
	cache.RemoveGroup("grp1")
	if len(cache.cache) != 2 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if len(cache.groups) != 1 {
		t.Errorf("Wrong intems in groups: %+v", cache.groups)
	} else if len(cache.groups["grp4"]) != 1 {
		t.Errorf("Wrong intems in group: %+v", cache.groups["grp4"])
	}
	cache.RemoveGroup("grp1")
	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}

}

func TestGetGroupItems(t *testing.T) {
	cache := NewCache(UnlimitedCaching, 0, false, false,
		[]func(itmID string, value any){func(itmID string, v interface{}) { lastEvicted = itmID }})
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, ci.groupIDs)
	}
	if grpItms := cache.GetGroupItems("grp1"); len(grpItms) != 3 {
		t.Errorf("wrong group items: %+v", grpItms)
	}
	if grpItms := cache.GetGroupItems("nonexsitent"); grpItms != nil {
		t.Errorf("wrong group items: %+v", grpItms)
	}
	if has := cache.HasGroup("grp1"); !has {
		t.Error("should have group")
	}
	if has := cache.HasGroup("nonexistent"); has {
		t.Error("should not have group")
	}
}

func TestSetGetRemLRU(t *testing.T) {
	cache := NewCache(3, 0, false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	if len(cache.cache) != 3 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != 3 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if cache.lruIdx.Front().Value.(*cachedItem).itemID != "_5_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	} else if cache.lruIdx.Back().Value.(*cachedItem).itemID != "_3_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != 3 {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != 0 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != 0 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
	if _, has := cache.Get("2"); has {
		t.Error("item still in cache")
	}
	// rewrite and reposition 3
	cache.Set("_3_", "third", nil)
	if val, has := cache.Get("_3_"); !has {
		t.Error("item not in cache")
	} else if val.(string) != "third" {
		t.Errorf("wrong item value: %v", val)
	}
	if cache.lruIdx.Len() != 3 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if cache.lruIdx.Front().Value.(*cachedItem).itemID != "_3_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	} else if cache.lruIdx.Back().Value.(*cachedItem).itemID != "_4_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	}
	cache.Set("_2_", "second", nil)
	if val, has := cache.Get("_2_"); !has {
		t.Error("item not in cache")
	} else if val.(string) != "second" {
		t.Errorf("wrong item value: %v", val)
	}
	if cache.lruIdx.Len() != 3 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if cache.lruIdx.Front().Value.(*cachedItem).itemID != "_2_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	} else if cache.lruIdx.Back().Value.(*cachedItem).itemID != "_5_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	}
	// 4 should have been removed
	if _, has := cache.Get("_4_"); has {
		t.Error("item still in cache")
	}
	cache.Remove("_2_")
	if _, has := cache.Get("_2_"); has {
		t.Error("item still in cache")
	}
	if len(cache.cache) != 2 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != 2 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != 2 {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.lruIdx.Front().Value.(*cachedItem).itemID != "_3_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	} else if cache.lruIdx.Back().Value.(*cachedItem).itemID != "_5_" {
		t.Errorf("Wrong order of items in the lru index: %+v", cache.lruIdx)
	}
	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
}

func TestSetGetRemTTLDynamic(t *testing.T) {
	cache := NewCache(UnlimitedCaching, time.Duration(10*time.Millisecond), false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	if len(cache.cache) != 5 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != 0 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != 0 {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != 5 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != 5 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
	if expTime, has := cache.GetItemExpiryTime(testCIs[0].itemID); !has {
		t.Errorf("cannot find item with id: %s", testCIs[0].itemID)
	} else if time.Now().After(expTime) {
		t.Errorf("wrong time replied: %v", expTime)
	}
	if has := cache.HasItem(testCIs[0].itemID); !has {
		t.Errorf("cannot find item with id: %s", testCIs[0].itemID)
	}
	if has := cache.HasItem("nonexistent"); has {
		t.Error("item should not be in")
	}
	time.Sleep(time.Duration(6 * time.Millisecond))
	if _, has := cache.Get("_2_"); !has {
		t.Error("item not in cache")
	}
	time.Sleep(time.Duration(6 * time.Millisecond))
	if cache.Len() != 1 {
		t.Errorf("Wrong items in cache: %+v", cache.cache)
	}
	if cache.ttlIdx.Len() != 1 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != 1 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
}

func TestSetGetRemTTLStatic(t *testing.T) {
	cache := NewCache(UnlimitedCaching, time.Duration(10*time.Millisecond), true, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	if cache.Len() != 5 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	time.Sleep(time.Duration(6 * time.Millisecond))
	if _, has := cache.Get("_2_"); !has {
		t.Error("item not in cache")
	}
	time.Sleep(time.Duration(6 * time.Millisecond))
	if cache.Len() != 0 {
		t.Errorf("Wrong items in cache: %+v", cache.cache)
	}
}

func TestSetGetRemLRUttl(t *testing.T) {
	nrItems := 3
	cache := NewCache(nrItems, time.Duration(10*time.Millisecond), false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	if cache.Len() != nrItems {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != nrItems {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != nrItems {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != nrItems {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != nrItems {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
	time.Sleep(time.Duration(6 * time.Millisecond))
	cache.Remove("_4_")
	cache.Set("_3_", "third", nil)
	nrItems = 2
	if cache.Len() != nrItems {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != nrItems {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != nrItems {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != nrItems {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != nrItems {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
	time.Sleep(time.Duration(6 * time.Millisecond)) // timeout items which were not modified
	nrItems = 1
	if cache.Len() != nrItems {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != nrItems {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != nrItems {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != nrItems {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != nrItems {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
}

func TestCacheDisabled(t *testing.T) {
	cache := NewCache(DisabledCaching, time.Duration(10*time.Millisecond), false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
		if _, has := cache.Get(ci.itemID); has {
			t.Errorf("Wrong intems in cache: %+v", cache.cache)
		}
	}
	if cache.Len() != 0 {
		t.Errorf("Wrong intems in cache: %+v", cache.cache)
	}
	if cache.lruIdx.Len() != 0 {
		t.Errorf("Wrong items in lru index: %+v", cache.lruIdx)
	}
	if len(cache.lruRefs) != 0 {
		t.Errorf("Wrong items in lru references: %+v", cache.lruRefs)
	}
	if cache.ttlIdx.Len() != 0 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlIdx)
	}
	if len(cache.ttlRefs) != 0 {
		t.Errorf("Wrong items in ttl index: %+v", cache.ttlRefs)
	}
	cache.Remove("4")
}

func TestCacheGroupLength(t *testing.T) {
	c := &Cache{
		groups: map[string]map[string]struct{}{
			"testGroupID": {
				"item1": {},
				"item2": {},
				"item3": {},
			},
		},
	}
	grpID := "testGroupID"

	exp := 3
	rcv := c.GroupLength(grpID)

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestCacheGetItemExpiryTime(t *testing.T) {
	c := &Cache{
		cache: map[string]*cachedItem{
			"otherItemID": {},
		},
	}
	itmID := "testItemID"

	var exp time.Time
	rcv, ok := c.GetItemExpiryTime(itmID)

	if ok != false {
		t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", false, ok)
	}

	if rcv != exp {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", exp, rcv)
	}
}

func TestCacheSetWithOffCollector(t *testing.T) {
	var logBuf bytes.Buffer
	c := NewCache(-1, 0, false, false, []func(itmID string, value any){func(itmID string, value interface{}) {}})
	c.offCollector = &OfflineCollector{
		collectSetEntity: true,
		collection: map[string]*CollectionEntity{
			"CacheID1": {},
		},
		logger: &testLogger{log.New(&logBuf, "", 0)},
	}

	c.Set("CacheID1", "CacheValue1", []string{"CacheGroup1"})
	if t1, ok := c.Get("CacheID1"); !ok || t1 != "CacheValue1" {
		t.Errorf("Expected <CacheValue1>, received <%+v>", t1)
	}
	if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
	expOC := &OfflineCollector{
		collectSetEntity: true,
		collection: map[string]*CollectionEntity{
			"CacheID1": {
				IsSet:  true,
				ItemID: "CacheID1",
			},
		},
		logger: c.offCollector.logger,
	}
	if !reflect.DeepEqual(expOC, c.offCollector) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC, c.offCollector)
	}
}

func TestCacheSetWithOffCollectorErr(t *testing.T) {
	var logBuf bytes.Buffer
	c := NewCache(-1, 0, false, false, []func(itmID string, value any){func(itmID string, value interface{}) {}})
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
	c.offCollector = &OfflineCollector{
		collectSetEntity: false,
		writeLimit:       1,
		file:             f,
		logger:           &testLogger{log.New(&logBuf, "", 0)},
	}

	c.Set("CacheID1", "CacheValue1", []string{"CacheGroup1"})
	if t1, ok := c.Get("CacheID1"); !ok || t1 != "CacheValue1" {
		t.Errorf("Expected <CacheValue1>, received <%+v>", t1)
	}
	expErr := "error getting file stat: stat /tmp/tmpfile: file already closed"
	if rcv := logBuf.String(); !strings.Contains(rcv, expErr) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expErr, rcv)
	}
}

func TestCacheDumpToFile(t *testing.T) {
	var logBuf bytes.Buffer
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
	c := NewCache(-1, 0, false, false, []func(itmID string, value any){func(itmID string, value interface{}) {}})
	c.cache["item1"] = &cachedItem{itemID: "item1", value: "val1", groupIDs: []string{"gr1"}}
	c.offCollector = &OfflineCollector{
		writeLimit:   1000,
		dumpInterval: 1,
		file:         file,
		writer:       writer,
		encoder:      encoder,
		logger:       &testLogger{log.New(&logBuf, "", 0)},
		collection: map[string]*CollectionEntity{
			"item1": {
				IsSet:  true,
				ItemID: "item1",
			},
			"item2": {
				IsSet:  false,
				ItemID: "item2",
			},
		},
	}
	if err := c.DumpToFile(); err != nil {
		t.Error(err)
	}
	file.Close()
	if !reflect.DeepEqual(map[string]*CollectionEntity{}, c.offCollector.collection) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", c.offCollector.collection)
	}
	if rcv := logBuf.String(); !strings.Contains(rcv, "") {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}

	files, err := os.ReadDir(path + "/*default")
	if err != nil {
		t.Error(err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file, received <%v>", files)
	}
	f, err := os.Open(path + "/*default/" + files[0].Name())
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
			IsSet:  false,
			ItemID: "item2",
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
	if err := dc.Decode(&rcv); err == nil || err.Error() != "EOF" {
		t.Error(err)
	}
}

func TestNewCacheFromFolderErr1(t *testing.T) {
	_, err := NewCacheFromFolder(&OfflineCollector{fldrPath: "/tmp/doesntExist"}, 0, 0, false, false, nil)
	expErr := "error walking the path: lstat /tmp/doesntExist: no such file or directory"
	if err == nil || expErr != err.Error() {
		t.Errorf("expected error <%+v>, received error <%+v>", expErr, err)
	}
}

func TestCacheAsyncDumpEntities(t *testing.T) {
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
	writer := bufio.NewWriter(tmpFile)
	cache := &Cache{
		maxEntries: -1,
		cache:      make(map[string]*cachedItem),
		groups:     make(map[string]map[string]struct{}),
		offCollector: &OfflineCollector{
			dumpInterval:     100 * time.Millisecond,
			dumpStopped:      make(chan struct{}),
			stopDump:         make(chan struct{}),
			file:             tmpFile,
			collectSetEntity: true,
			collection:       make(map[string]*CollectionEntity),
			fldrPath:         "/tmp/internal_db/*default",
			writer:           writer,
			encoder:          gob.NewEncoder(writer),
		},
	}
	cache.Set("CacheID", "sampleValue", []string{"CacheGroup1"})
	go func() {
		cache.asyncDumpEntities()
		exp := &cachedItem{
			itemID:     "CacheID",
			value:      "sampleValue",
			expiryTime: time.Time{},
			groupIDs:   []string{"CacheGroup1"},
		}
		if !reflect.DeepEqual(exp, cache.cache["CacheID"]) {
			t.Errorf("Expected <%+v>, Received <%+v>", exp, cache.cache["CacheID"])
		}
	}()
	time.Sleep(150 * time.Millisecond)
	cache.offCollector.stopDump <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

// BenchmarkSetSimpleCache 	10000000	       228 ns/op
func BenchmarkSetSimpleCache(b *testing.B) {
	cache := NewCache(UnlimitedCaching, 0, false, false, nil)
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Set(ci.itemID, ci.value, nil)
	}
}

// BenchmarkGetSimpleCache 	20000000	        99.7 ns/op
func BenchmarkGetSimpleCache(b *testing.B) {
	cache := NewCache(UnlimitedCaching, 0, false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Get(ci.itemID)
	}
}

// BenchmarkSetLRU         	 5000000	       316 ns/op
func BenchmarkSetLRU(b *testing.B) {
	cache := NewCache(3, 0, false, false, nil)
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Set(ci.itemID, ci.value, nil)
	}
}

// BenchmarkGetLRU         	20000000	       114 ns/op
func BenchmarkGetLRU(b *testing.B) {
	cache := NewCache(3, 0, false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Get(ci.itemID)
	}
}

// BenchmarkSetTTL         	50000000	        30.4 ns/op
func BenchmarkSetTTL(b *testing.B) {
	cache := NewCache(0, time.Duration(time.Millisecond), false, false, nil)
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Set(ci.itemID, ci.value, nil)
	}
}

// BenchmarkGetTTL         	20000000	        88.4 ns/op
func BenchmarkGetTTL(b *testing.B) {
	cache := NewCache(0, time.Duration(5*time.Millisecond), false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Get(ci.itemID)
	}
}

// BenchmarkSetLRUttl      	 5000000	       373 ns/op
func BenchmarkSetLRUttl(b *testing.B) {
	cache := NewCache(3, time.Duration(time.Millisecond), false, false, nil)
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Set(ci.itemID, ci.value, nil)
	}
}

// BenchmarkGetLRUttl      	10000000	       187 ns/op
func BenchmarkGetLRUttl(b *testing.B) {
	cache := NewCache(3, time.Duration(5*time.Millisecond), false, false, nil)
	for _, ci := range testCIs {
		cache.Set(ci.itemID, ci.value, nil)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 0, len(testCIs)-1 // so we can have random index
	for n := 0; n < b.N; n++ {
		ci := testCIs[rand.Intn(max-min)+min]
		cache.Get(ci.itemID)
	}
}
