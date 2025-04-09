/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.
*/

package ltcache

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestPopulateEncodersErr(t *testing.T) {
	expErr := "no such file or directory"
	if _, _, _, err := populateEncoder("/tmp/testOff/*default", ""); err == nil ||
		!strings.Contains(err.Error(), expErr) {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestOfflineCollectorWriteEntity(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", tmpRewriteName+"-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	oneMibString := strings.Repeat("writing", 1_048_576/len("writing")+1)
	tmpFile.WriteString(oneMibString)
	oc := &OfflineCollector{
		fileSizeLimit: 1,
		fldrPath:      path + "/*default",
		file:          tmpFile,
	}
	if err := oc.writeEntity(&OfflineCacheEntity{}); err != nil {
		t.Error(err)
	} else if !strings.HasPrefix(oc.file.Name(), "/tmp/internal_db/*default/"+tmpRewriteName) {
		t.Errorf("expected new file, received <%v>", oc.file.Name())
	} else if oc.writer == nil {
		t.Errorf("expected new writer, received nil")
	} else if oc.encoder == nil {
		t.Errorf("expected new encoder, received nil")
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
			ItemID:   "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		})
	}
	oceMap := map[string]*OfflineCacheEntity{}
	handleEntity := func(oce *OfflineCacheEntity) { // will add/delete OfflineCacheEntity from oceMap
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	exp := map[string]*OfflineCacheEntity{
		"testID": {IsSet: true, ItemID: "testID", Value: "value",
			GroupIDs: []string{"gpID"}},
	}
	if err := readAndDecodeFile(path+"/file", handleEntity); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, oceMap) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, oceMap)
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
			ItemID:   "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		})
	}
	oceMap := map[string]*OfflineCacheEntity{
		"testID": {
			IsSet:    false,
			ItemID:   "testID",
			Value:    "value",
			GroupIDs: []string{"gpID"},
		},
	}
	handleEntity := func(oce *OfflineCacheEntity) { // will add/delete OfflineCacheEntity from oceMap
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	exp := map[string]*OfflineCacheEntity{}
	if err := readAndDecodeFile(path+"/file", handleEntity); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(exp, oceMap) {
		t.Errorf("Expected <%+v>, Received <%+v>", exp, oceMap)
	}
}

func TestReadAndDecodeFileErr1(t *testing.T) {
	expErr := "error opening file <> in memory: open : no such file or directory"
	oceMap := map[string]*OfflineCacheEntity{}
	handleEntity := func(oce *OfflineCacheEntity) {
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	if err := readAndDecodeFile("", handleEntity); err == nil ||
		err.Error() != expErr {
		t.Errorf("Expected error <%v>, Received <%v>", expErr, err)
	}
}

func TestOfflineCollectorCollect(t *testing.T) {
	oc := &OfflineCollector{
		collection: map[string]*CollectionEntity{},
	}
	expOC := &OfflineCollector{
		collection: map[string]*CollectionEntity{
			"CacheID1": {
				IsSet:  true,
				ItemID: "CacheID1",
			},
		},
	}
	oc.collect("CacheID1")
	if !reflect.DeepEqual(expOC.collection, oc.collection) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", expOC.collection, oc.collection)
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
	tmpFile.WriteString("writing")
	if newf, w, e, err := rotateFileIfNeeded(path+"/*default", 0, tmpFile); err != nil {
		t.Error(err)
	} else if newf == nil {
		t.Errorf("expected new file, received nil")
	} else if w == nil {
		t.Errorf("expected new writer, received nil")
	} else if e == nil {
		t.Errorf("expected new encoder, received nil")
	}
}

func TestOfflineCollectorCheckAndRotateFileErr(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", tmpRewriteName+"-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpFile.WriteString("writing")
	if newf, w, e, err := rotateFileIfNeeded(path+"/*default", 0, tmpFile); err != nil {
		t.Error(err)
	} else if newf == nil {
		t.Errorf("expected new file, received nil")
	} else if w == nil {
		t.Errorf("expected new writer, received nil")
	} else if e == nil {
		t.Errorf("expected new encoder, received nil")
	}
}

func TestOfflineCollectorCheckAndRotateFileNoLimit(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", tmpRewriteName+"-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	tmpFile.WriteString("writing")
	if newf, w, e, err := rotateFileIfNeeded(path+"/*default", 1000, tmpFile); err != nil {
		t.Error(err)
	} else if newf != nil {
		t.Errorf("expected new file, received nil")
	} else if w != nil {
		t.Errorf("expected new writer, received nil")
	} else if e != nil {
		t.Errorf("expected new encoder, received nil")
	}
}

func TestOfflineCollectorStoreRemoveEntityNoInterval(t *testing.T) {
	path := "/tmp/internal_db"
	if err := os.MkdirAll(path+"/*default", 0755); err != nil {
		t.Fatal(err)
	}
	tmpFile, err := os.CreateTemp("/tmp/internal_db/*default", tmpRewriteName+"-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	var encBuf bytes.Buffer
	oc := &OfflineCollector{
		fileSizeLimit: 1000,
		file:          tmpFile,
		writer:        bufio.NewWriter(&bytes.Buffer{}),
		encoder:       gob.NewEncoder(&encBuf),
	}
	bufExpect := "OfflineCacheEntity"
	oc.storeRemoveEntity("CacheID1", -1)
	if rcv := encBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected to contain <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorStoreRemoveEntityErr1(t *testing.T) {
	var logBuf bytes.Buffer
	oc := &OfflineCollector{
		fileSizeLimit: 1,
		logger:        &testLogger{log.New(&logBuf, "", 0)},
	}
	bufExpect := "error getting file stat: invalid argument"
	oc.storeRemoveEntity("CacheID1", -1)
	if rcv := logBuf.String(); !strings.Contains(rcv, bufExpect) {
		t.Errorf("Expected <%+v>, \nReceived <%+v>", bufExpect, rcv)
	}
}

func TestOfflineCollectorStoreRemoveEntityInterval(t *testing.T) {
	oc := &OfflineCollector{
		collection: map[string]*CollectionEntity{},
	}
	exp := &CollectionEntity{
		IsSet:  false,
		ItemID: "CacheID1",
	}
	oc.storeRemoveEntity("CacheID1", 1)
	if !reflect.DeepEqual(exp, oc.collection["CacheID1"]) {
		t.Errorf("Expected <%+v>, \nreceived <%+v>", exp, oc.collection["CacheID1"])
	}
}

func TestOfflineCollectorShouldSkipRewriteTrue(t *testing.T) {
	path := "/tmp/internal_db/*default"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+"/otherFile", os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		f.Close()
		if err := os.RemoveAll("/tmp/internal_db"); err != nil {
			t.Errorf("Failed to delete temporary dir: %v", err)
		}
	}()
	fName := f.Name()
	oc := OfflineCollector{
		file:     f,
		fldrPath: path,
	}
	if shouldSkipRewrite([]string{oc.fldrPath + "/otherFile"}, "") &&
		fName != oc.file.Name() {
		t.Errorf("Expected file <%s>, received <%s>", fName, oc.file.Name())
	}
}

func TestOfflineCollectorShouldSkipRewriteFalse(t *testing.T) {
	path := "/tmp/internal_db/*default"
	if shouldSkipRewrite([]string{path + "/file1", path + "/file2"},
		"") {
		t.Errorf("Expected shouldSkipRewrite false, received true")
	}
}
