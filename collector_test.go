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
	"log"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEnsureDir(t *testing.T) {
	path := "/tmp/testEnsureDir"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if err := ensureDir(path); err != nil {
		t.Error(err)
	}
}

func TestEnsureDirCreation(t *testing.T) {
	path := "/tmp/testEnsureDir"
	if err := ensureDir(path); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Error(err)
	}
}

func TestEnsureDirErr(t *testing.T) {
	path := "/root/testEnsureDir"
	expErr := "stat /root/testEnsureDir: permission denied"
	if err := ensureDir(path); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestPopulateEncoders(t *testing.T) {
	offC := &OfflineCollector{
		files:    make(map[string]*os.File),
		writers:  make(map[string]*bufio.Writer),
		encoders: make(map[string]*gob.Encoder),
	}
	if err := os.MkdirAll("/tmp/testOff/*default", 0755); err != nil {
		t.Error(err)
		return
	}
	if err := offC.populateEncoders("/tmp/testOff/*default"); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll("/tmp/testOff"); err != nil {
		t.Error(err)
		return
	}
}

func TestPopulateEncodersErr(t *testing.T) {
	offC := &OfflineCollector{
		files: map[string]*os.File{
			"*default": nil,
		},
	}
	expErr := "no such file or directory"
	if err := offC.populateEncoders("/tmp/testOff/*default"); err == nil ||
		!strings.Contains(err.Error(), expErr) {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}

}
func TestProcessDumpFiles(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	if f, err := os.OpenFile(path+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644); err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	defer os.RemoveAll(path)
	tc := NewTransCacheWithOfflineCollector("/tmp", -1, -1, -1,
		map[string]*CacheConfig{}, nopLogger{})
	if tc.cache["*default"] != nil {
		t.Errorf("Expected tc to be nil, Received <%v>", tc.cache["*default"])
	}
	if err := processDumpFiles("*default", "/tmp", -1, -1, false, tc,
		&sync.RWMutex{}); err != nil {
		t.Error(err)
	}
	if tc.cache["*default"] == nil {
		t.Errorf("Expected tc to be populated, Received <%v>", tc.cache["*default"])
	}
}

func TestProcessDumpFilesErr1(t *testing.T) {
	expErr := "error walking the path: lstat /tmp/*default: no such file or directory"
	if err := processDumpFiles("*default", "/tmp", -1, -1, false, &TransCache{},
		nil); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestProcessDumpFilesErr2(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	if f, err := os.OpenFile(path+"/file",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		f.WriteString("unDecodable")
	}
	defer os.RemoveAll(path)
	expErr := "failed to decode OfflineCacheEntity at </tmp/*default/file>: unexpected EOF"
	if err := processDumpFiles("*default", "/tmp", -1, -1, false,
		NewTransCache(map[string]*CacheConfig{}), &sync.RWMutex{}); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestValidateFilePathsOldRewriteName(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path+"/0Rewrite", 0755); err != nil {
		t.Fatal(err)
	}
	if validPaths, err := validateFilePaths([]string{path + "/oldRewrite",
		path + "/0Rewrite"}, path); err != nil {
		t.Error(err)
	} else if !slices.Contains(validPaths, "/tmp/*default/oldRewrite") {
		t.Errorf("Expected <%+v>, Received <%+v>",
			[]string{"/tmp/*default/oldRewrite"}, validPaths)
	}
	if _, err := os.Stat(path + "/0Rewrite"); !os.IsNotExist(err) {
		t.Errorf("Expected folder to be deleted, received error <%v>", err)
	}
}
func TestValidateFilePathsRewriteFileErr(t *testing.T) {
	path := "/tmp/*default"
	expErr := "remove /tmp/*default/0Rewrite: no such file or directory"
	if _, err := validateFilePaths([]string{path + "/oldRewrite",
		path + "/0Rewrite"}, path); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestValidateFilePathsTmpRewrite(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path+"/tmpRewrite", 0755); err != nil {
		t.Fatal(err)
	}
	expErr := "remove /tmp/*default/tmpRewrite: no such file or directory"
	if validPaths, err := validateFilePaths([]string{path +
		"/tmpRewrite"}, path); err != nil {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	} else if len(validPaths) != 0 {
		t.Errorf("Expected <%+v>, Received <%+v>", []string{}, validPaths)
	}
	if _, err := os.Stat(path + "/tmpRewrite"); !os.IsNotExist(err) {
		t.Errorf("Expected folder to be deleted, received error <%v>", err)
	}
}

func TestValidateFilePathsTmpRewriteErr(t *testing.T) {
	path := "/tmp/*default"
	expErr := "remove /tmp/*default/tmpRewrite: no such file or directory"
	if _, err := validateFilePaths([]string{path + "/tmpRewrite"}, path); err == nil ||
		err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}
func TestReadAndDecodeFileDecodeSet(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if f, err := os.OpenFile(path+"/file",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		enc := gob.NewEncoder(f)
		enc.Encode(OfflineCacheEntity{
			IsSet:    true,
			CacheID:  "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		})
	}
	oce := map[string]*OfflineCacheEntity{}
	exp := map[string]*OfflineCacheEntity{
		"testID": {IsSet: true, CacheID: "testID", Value: "value",
			GroupIDs: []string{"gpID"}},
	}
	if err := readAndDecodeFile(path+"/file", oce); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, oce) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, oce)
	}
}

func TestReadAndDecodeFileDecodeRemove(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	if f, err := os.OpenFile(path+"/file",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		enc := gob.NewEncoder(f)
		enc.Encode(OfflineCacheEntity{
			IsSet:    false,
			CacheID:  "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		})
	}
	oce := map[string]*OfflineCacheEntity{
		"testID": {
			IsSet:    false,
			CacheID:  "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		},
	}
	exp := map[string]*OfflineCacheEntity{}
	if err := readAndDecodeFile(path+"/file", oce); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, oce) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, oce)
	}
}

func TestReadAndDecodeFileErr1(t *testing.T) {
	expErr := "error opening file <> in memory: open : no such file or directory"
	if err := readAndDecodeFile("", map[string]*OfflineCacheEntity{}); err == nil ||
		err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestOfflineCacheEntityToCachedItem(t *testing.T) {
	oce := &OfflineCacheEntity{
		IsSet:    false,
		CacheID:  "testID",
		Value:    "value",
		GroupIDs: []string{"gpID"},
	}
	cI := oce.toCachedItem()
	exp := &cachedItem{
		itemID:   "testID",
		value:    "value",
		groupIDs: []string{"gpID"},
	}
	if !reflect.DeepEqual(exp, cI) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, cI)
	}
}

func TestOfflineCacheSetDisabledCaching(t *testing.T) {
	c := &Cache{maxEntries: DisabledCaching}
	exp := c
	c.set("", &cachedItem{})
	if !reflect.DeepEqual(exp, c) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, c)
	}
}

func TestOfflineCacheSetUnlimitedCaching(t *testing.T) {
	c := &Cache{lruRefs: make(map[string]*list.Element), maxEntries: 1, lruIdx: list.New()}
	c.set("cacheID", &cachedItem{itemID: "pushedItem", value: "val"})
	c.set("cacheID", &cachedItem{itemID: "pushedItem2", value: "val2"})
	exp := &Cache{lruRefs: make(map[string]*list.Element), lruIdx: list.New()}
	exp.lruRefs["cacheID"] = exp.lruIdx.PushFront(&cachedItem{
		itemID: "pushedItem", value: "val"})
	exp.lruRefs["cacheID"] = exp.lruIdx.PushFront(&cachedItem{
		itemID: "pushedItem2", value: "val2"})
	if !reflect.DeepEqual(exp.lruRefs["cacheID"], c.lruRefs["cacheID"]) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp.lruRefs["cacheID"].Value,
			c.lruRefs["cacheID"].Value)
	}
}

func TestOfflineCacheSetTTL(t *testing.T) {
	c := &Cache{ttlRefs: make(map[string]*list.Element), maxEntries: -1,
		ttlIdx: list.New(), ttl: 1}
	c.set("cacheID", &cachedItem{itemID: "pushedItem", value: "val"})
	exp := &Cache{ttlRefs: make(map[string]*list.Element), ttlIdx: list.New()}
	exp.ttlRefs["cacheID"] = exp.ttlIdx.PushFront(&cachedItem{
		itemID: "pushedItem", value: "val"})
	if !reflect.DeepEqual(exp.ttlRefs["cacheID"], c.ttlRefs["cacheID"]) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp.ttlRefs["cacheID"],
			c.ttlRefs["cacheID"])
	}
}

func TestNewCacheFromDump(t *testing.T) {
	offE := map[string]*OfflineCacheEntity{
		"CacheID1": {
			IsSet:    true,
			CacheID:  "CacheID1",
			Value:    "val",
			GroupIDs: []string{"gr1"},
		},
	}
	c := newCacheFromDump(offE, 0, 1*time.Second, true, func(itmID string, value interface{}) {})
	expC := &Cache{
		cache: map[string]*cachedItem{
			"CacheID1": {
				itemID:   "CacheID1",
				value:    "val",
				groupIDs: []string{"gr1"},
			},
		},
		groups: map[string]map[string]struct{}{
			"gr1": {
				"CacheID1": {},
			},
		},
	}
	if !reflect.DeepEqual(expC.cache, c.cache) {
		t.Errorf("Expected <%+v>, Received <%+v>", expC.cache, c.cache)
	}
}

func TestOfflineCollectorCollect(t *testing.T) {
	oc := &OfflineCollector{
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		setColl:    map[string]map[string]*OfflineCacheEntity{"*default": nil},
	}
	expOC := &OfflineCollector{
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {
				"CacheID1": {
					IsSet:   true,
					CacheID: "CacheID1",
				},
			},
		},
	}
	oc.collect("*default", "CacheID1")
	if !reflect.DeepEqual(expOC.setColl, oc.setColl) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.setColl, oc.setColl)
	}
}

func TestClearOfflineInstance(t *testing.T) {
	path := "/tmp/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	var logBuf bytes.Buffer
	oc := &OfflineCollector{
		folderPath: "/tmp",
		files:      make(map[string]*os.File),
		writers:    make(map[string]*bufio.Writer),
		encoders:   make(map[string]*gob.Encoder),
		logger:     &testLogger{log.New(&logBuf, "", 0)},
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {
				"CacheID1": {
					IsSet:   true,
					CacheID: "CacheID1",
				},
			},
		},
		remColl: map[string][]string{"*default": {"CacheID2"}},
	}
	oc.clearOfflineInstance("*default")
	expOC := &OfflineCollector{
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {},
		},
		remColl: map[string][]string{"*default": {}},
	}
	if !reflect.DeepEqual(expOC.setColl, oc.setColl) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.setColl, oc.setColl)
	} else if !reflect.DeepEqual(expOC.remColl, oc.remColl) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.remColl, oc.remColl)
	} else if rcv := logBuf.String(); rcv != "" {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", "", rcv)
	}
}

type testLogger struct {
	*log.Logger
}

func (l *testLogger) Alert(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Close() error {
	return nil
}
func (l *testLogger) Crit(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Debug(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Emerg(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Err(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Notice(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Warning(msg string) error {
	l.Println(msg)
	return nil
}
func (l *testLogger) Info(msg string) error {
	l.Println(msg)
	return nil
}

func TestClearOfflineInstanceErr1(t *testing.T) {
	var logBuf bytes.Buffer
	oc := &OfflineCollector{
		logger:     &testLogger{log.New(&logBuf, "", 0)},
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {
				"CacheID1": {
					IsSet:   true,
					CacheID: "CacheID1",
				},
			},
		},
		remColl: map[string][]string{"*default": {"CacheID2"}},
	}
	bufExpect := "Error walking the path: lstat /*default: no such file or directory"
	oc.clearOfflineInstance("*default")
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorWriteRemoveEntity(t *testing.T) {
	oc := &OfflineCollector{
		remColl:    map[string][]string{"*default": {"CacheID2"}},
		writeLimit: -1,
		writers:    map[string]*bufio.Writer{"*default": bufio.NewWriter(&bytes.Buffer{})},
		encoders:   map[string]*gob.Encoder{"*default": gob.NewEncoder(&bytes.Buffer{})},
	}
	if err := oc.writeRemoveEntity("*default"); err != nil {
		t.Error(err)
	} else if len(oc.remColl) != 0 {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", nil, oc.remColl)
	}
}

func TestOfflineCollectorWriteRemoveEntityErr1(t *testing.T) {
	oc := &OfflineCollector{
		remColl: map[string][]string{"*default": {"CacheID2"}},
	}
	expErr := "error getting file stat: invalid argument"
	if err := oc.writeRemoveEntity("*default"); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestOfflineCollectorCheckAndRotateFile(t *testing.T) {
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
	oc := &OfflineCollector{
		folderPath: "/tmp/internal_db",
		files:      map[string]*os.File{"*default": tmpFile},
		writers:    make(map[string]*bufio.Writer),
		encoders:   make(map[string]*gob.Encoder),
		writeLimit: -2,
	}
	if err := oc.checkAndRotateFile("*default"); err != nil {
		t.Error(err)
	} else if _, has := oc.writers["*default"]; !has {
		t.Errorf("Expected *default writer to be created, \nReceived <%+v>", oc.writers)
	} else if _, has := oc.encoders["*default"]; !has {
		t.Errorf("Expected *default writer to be created, \nReceived <%+v>", oc.encoders)
	}
}

func TestOfflineCollectorCheckAndRotateFileErr(t *testing.T) {
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
	oc := &OfflineCollector{
		files:      map[string]*os.File{"*default": tmpFile},
		writeLimit: -2,
	}
	expErr := "no such file or directory"
	if err := oc.checkAndRotateFile("*default"); err == nil ||
		!strings.Contains(err.Error(), expErr) {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestOfflineCollectorStoreCacheDumpInterval0(t *testing.T) {
	oc := &OfflineCollector{
		dumpInterval: 0,
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {
				"CacheID1": {
					IsSet:   true,
					CacheID: "CacheID1",
				},
			},
		},
	}
	expOC := oc
	if err := oc.storeCache("", "", "", time.Time{}, []string{}); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(expOC, oc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC, oc)
	}
}

func TestOfflineCollectorStoreCacheDumpIntervalMinus1(t *testing.T) {
	encBuf := &bytes.Buffer{}
	writeBuf := &bytes.Buffer{}
	oc := &OfflineCollector{
		setCollMux:   map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		dumpInterval: -1,
		writeLimit:   -1,
		encoders:     map[string]*gob.Encoder{"*default": gob.NewEncoder(encBuf)},
		writers:      map[string]*bufio.Writer{"*default": bufio.NewWriter(writeBuf)},
	}
	expOC := oc
	if err := oc.storeCache("*default", "CacheID1", "CacheValue1", time.Time{},
		[]string{"CacheGroup1"}); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(expOC, oc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC, oc)
	}

	dec := gob.NewDecoder(encBuf)
	var oce *OfflineCacheEntity
	expOCE := &OfflineCacheEntity{
		IsSet:      true,
		CacheID:    "CacheID1",
		Value:      "CacheValue1",
		GroupIDs:   []string{"CacheGroup1"},
		ExpiryTime: time.Time{},
	}
	if err := dec.Decode(&oce); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(expOCE, oce) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOCE, oce)
	}
	if writeBuf.String() != "" {
		t.Errorf("Expected empty writeBuf, received <%s>", writeBuf.String())
	}
}

func TestOfflineCollectorStoreCacheDumpInterval(t *testing.T) {
	oc := &OfflineCollector{
		dumpInterval: time.Second,
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {},
		},
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
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
	}
	if err := oc.storeCache("*default", "CacheID1", "CacheValue1", time.Time{},
		[]string{"CacheGroup1"}); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(expOC, oc) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC, oc)
	}
}

func TestOfflineCollectorStoreSetEntityErr1(t *testing.T) {
	oc := &OfflineCollector{}
	expErr := "error getting file stat: invalid argument"
	if err := oc.storeSetEntity("*default", "CacheID1", "CacheValue1", time.Time{},
		[]string{"CacheGroup1"}); err == nil || err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestOfflineCollectorStoreRemoveEntityNoInterval(t *testing.T) {
	var encBuf bytes.Buffer
	oc := &OfflineCollector{
		dumpInterval: -1,
		writeLimit:   -1,
		setCollMux:   map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		writers:      map[string]*bufio.Writer{"*default": bufio.NewWriter(&bytes.Buffer{})},
		encoders:     map[string]*gob.Encoder{"*default": gob.NewEncoder(&encBuf)},
	}
	bufExpect := "OfflineCacheEntity"
	oc.storeRemoveEntity("*default", "CacheID1")
	if rcv := encBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorStoreRemoveEntityErr1(t *testing.T) {
	var logBuf bytes.Buffer
	oc := &OfflineCollector{
		dumpInterval: -1,
		setCollMux:   map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		remColl:      make(map[string][]string),
		logger:       &testLogger{log.New(&logBuf, "", 0)},
	}
	bufExpect := "error getting file stat: invalid argument"
	oc.storeRemoveEntity("*default", "CacheID1")
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorStoreRemoveEntityInterval(t *testing.T) {
	oc := &OfflineCollector{
		setCollMux: map[string]*sync.RWMutex{"*default": new(sync.RWMutex)},
		setColl: map[string]map[string]*OfflineCacheEntity{
			"*default": {
				"CacheID1": {
					IsSet:   true,
					CacheID: "CacheID1",
				},
			},
		},
		remColl: make(map[string][]string),
	}
	exp := map[string][]string{
		"*default": {"CacheID1"},
	}
	oc.storeRemoveEntity("*default", "CacheID1")
	if len(oc.setColl["*default"]) != 0 {
		t.Errorf("Expected no keys in setColl, received <%+v>", oc.setColl["*default"])
	} else if !reflect.DeepEqual(exp, oc.remColl) {
		t.Errorf("Expected <%+v>, received <%+v>", exp, oc.remColl)
	}
}

func TestCloseFileOK(t *testing.T) {
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
	if err := closeFile(f); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(path + "/file"); !os.IsNotExist(err) {
		t.Error("Expected file to be removed")
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
	expErr := "error getting stats for file </tmp/*default/file>: stat /tmp/*default/file: file already closed"
	if err := closeFile(f); err.Error() != expErr {
		t.Errorf("Expected error <%+v>, \nReceived error <%+v>", expErr, err)
	}
}

func TestOCRunRewriteOff(t *testing.T) {
	oc := &OfflineCollector{
		rewriteInterval: -1,
		rewriteStopped:  make(chan struct{}),
	}
	go func() {
		oc.runRewrite()

	}()
	select {
	case <-oc.rewriteStopped:
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected rewrite to be stopped")
	}
}

func TestOCRunRewriteStop(t *testing.T) {
	oc := &OfflineCollector{
		rewriteStopped:  make(chan struct{}),
		stopRewrite:     make(chan struct{}),
		rewriteInterval: 1 * time.Second,
	}
	go func() {
		oc.runRewrite()
	}()
	time.Sleep(50 * time.Millisecond)
	oc.stopRewrite <- struct{}{}
	select {
	case <-oc.rewriteStopped:
	case <-time.After(200 * time.Second):
		t.Error("Expected rewrite to be stopped, but it wasn't")
	}
}

func TestOCRewriteErr1(t *testing.T) {
	var logBuf bytes.Buffer
	oc := OfflineCollector{
		folderPath: "/tmp/notexistent",
		logger:     &testLogger{log.New(&logBuf, "", 0)},
	}
	bufExpect := "Failed to read dir </tmp/notexistent> error: open /tmp/notexistent: no such file or directory"
	oc.rewrite()
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOCRewriteShouldContinue(t *testing.T) {
	path := "/tmp/internal_db/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/0Rewrite1", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	f.Close()
	f, err = os.OpenFile(path+"/otherFile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
	}
	defer func() {
		f.Close()
		if err := os.RemoveAll("/tmp/internal_db"); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var encBuf bytes.Buffer
	oc := OfflineCollector{
		writeLimit: 1,
		files: map[string]*os.File{
			"*default": f,
		},
		folderPath: "/tmp/internal_db",
		writers:    make(map[string]*bufio.Writer),
		encoders:   make(map[string]*gob.Encoder),
	}
	oc.writers["*default"] = bufio.NewWriter(oc.files["*default"])
	oc.encoders["*default"] = gob.NewEncoder(oc.writers["*default"])
	if err := oc.storeSetEntity("*default", "CacheID", "sampleValue", time.Time{}, []string{"CacheGroup1"}); err != nil {
		t.Error(err)
	}
	written, err := os.ReadFile("/tmp/internal_db/*default/otherFile")
	if err != nil {
		log.Fatal(err)
	}
	exp := "OfflineCacheEntity"
	oc.rewrite()
	if rcv := encBuf.String(); !strings.Contains(string(written), exp) {
		t.Errorf("Expected nothing to be written <%+v>, \nReceived <%+v>", exp, rcv)
	}
}
