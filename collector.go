/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

const (
	rewriteFileName = "0Rewrite" // prefix of the name used for files that have been
	// rewritten, starting with 0 to give natural directory walking priority
	tmpRewriteName = "tmpRewrite" // prefix of the name of files which are in the
	// process of being rewritten
	oldRewriteName = "oldRewrite" // prefix of the name of files to be deleted after
	// renewing rewrite files
)

// OfflineCollector used dump cache to files
type OfflineCollector struct {
	collMux    sync.RWMutex // lock collection so we dont dump while modifying them
	rewriteMux sync.RWMutex // lock rewriting process
	fileMux    sync.RWMutex // used to lock the maps file, writer and encoder, so we dont
	//  have concurrency while writing/reading
	collection map[string]*CollectionEntity // map[cacheItemKey]*CollectionEntity  Collects all
	// cache items SET/REMOVE-d from cache. A CollectionEntity holds only the most up to date cacheItem
	// and can either be a set or a remove at a time.
	fldrPath         string        // path to a Cache instance dump folder
	backupPath       string        // path where to backup Caches dump folder
	collectSetEntity bool          // decides weather to collect or write the SET cache command
	file             *os.File      // holds the file opened
	writer           *bufio.Writer // holds the buffer writer
	encoder          *gob.Encoder  // holds encoder
	writeLimit       int           // maximum size in bytes that can be written in a singular dump file
	logger           logger
	dumpInterval     time.Duration // holds duration to wait until next dump
	stopDump         chan struct{} // Used to stop cache dumping inverval
	dumpStopped      chan struct{} // signal when writing is finished
	rewriteInterval  time.Duration // holds duration to wait until next rewrite
	stopRewrite      chan struct{} // Used to stop inverval rewriting
	rewriteStopped   chan struct{} // signal when rewriting is finished
}

// NewOfflineCollector construct a new OfflineCollector
func NewOfflineCollector(cacheName string, opts *TransCacheOpts, logger logger) *OfflineCollector {
	return &OfflineCollector{
		collection:       make(map[string]*CollectionEntity),
		fldrPath:         path.Join(opts.DumpPath, cacheName),
		backupPath:       opts.BackupPath,
		writeLimit:       opts.WriteLimit,
		collectSetEntity: (opts.DumpInterval != -1),
		logger:           logger,
		dumpInterval:     opts.DumpInterval,
		rewriteInterval:  opts.RewriteInterval,
		stopDump:         make(chan struct{}),
		dumpStopped:      make(chan struct{}),
		stopRewrite:      make(chan struct{}),
		rewriteStopped:   make(chan struct{}),
	}
}

// CollectionEntity is used to temporarily collect cache keys of the items to be dumped to file
type CollectionEntity struct {
	IsSet  bool   // Controls if the item that is collected is a SET or a REMOVE of the item from cache
	ItemID string // Holds the cache ItemID
}

// OfflineCacheEntity is used as the structure to be encoded/decoded per cache item to be dumped to file
type OfflineCacheEntity struct {
	IsSet      bool      // Controls if the item that is written is a SET or a REMOVE of the item
	ItemID     string    // Holds the cache ItemID to be stored in file
	Value      any       // Value of cache item to be stored in file
	GroupIDs   []string  // GroupIDs of cache item to be stored in file
	ExpiryTime time.Time // ExpiryTime of cache item to be stored in file
}

type logger interface {
	Alert(string) error
	Close() error
	Crit(string) error
	Debug(string) error
	Emerg(string) error
	Err(string) error
	Info(string) error
	Notice(string) error
	Warning(string) error
}

type nopLogger struct{}

func (nopLogger) Alert(string) error   { return nil }
func (nopLogger) Close() error         { return nil }
func (nopLogger) Crit(string) error    { return nil }
func (nopLogger) Debug(string) error   { return nil }
func (nopLogger) Emerg(string) error   { return nil }
func (nopLogger) Err(string) error     { return nil }
func (nopLogger) Info(string) error    { return nil }
func (nopLogger) Notice(string) error  { return nil }
func (nopLogger) Warning(string) error { return nil }

// populateEncoder will create and open a new dump file in the provided fldrPath with
// prefix filePrefix, create an encoder and writer for it, and return them
func populateEncoder(fldrPath string, filePrefix string) (file *os.File,
	writer *bufio.Writer, encoder *gob.Encoder, err error) {
	filePath := filepath.Join(fldrPath, filePrefix+
		strconv.FormatInt(time.Now().UnixMilli(), 10)) // path of the dump file of current caching
	// instance, in miliseconds in case another dump happens within the second of the dump file created
	file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, nil, err
	}
	writer = bufio.NewWriter(file)
	encoder = gob.NewEncoder(writer)
	return
}

// validateFilePaths makes sure we dont recover from dump files that were stopped mid way rewriting
func validateFilePaths(paths []string, fileName string) (validPaths []string, err error) {
	// if there are paths with "oldRewrite" prefix, recover from them instead of 0Rewrite
	// having an oldRewrite still in the tree means the rewriting process was interupted
	var removeZeroRewrite bool // true if prefix oldRewrite was found in name of files
	for _, s := range paths {
		if strings.HasPrefix(s, path.Join(fileName, oldRewriteName)) {
			removeZeroRewrite = true
			break
		}
	}
	for _, s := range paths {
		// dont include "tmpRewrite" paths
		if strings.HasPrefix(s, path.Join(fileName, tmpRewriteName)) {
			if err := os.Remove(s); err != nil {
				return nil, err
			}
			continue
		}
		// dont include"0Rewrite" files if any "oldRewrite" found in tree
		if removeZeroRewrite && strings.HasPrefix(s, path.Join(fileName, rewriteFileName)) {
			if err := os.Remove(s); err != nil {
				return nil, err
			}
			continue
		}
		validPaths = append(validPaths, s)
	}
	return
}

// getFilePaths gets all file paths from dir directory
func getFilePaths(dir string) ([]string, error) {
	var filePaths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		filePaths = append(filePaths, path)
		return nil
	})
	return filePaths, err
}

// readAndDecodeFile reads dump file and decodes into OfflineCacheEntity to be used by handleEntity function
func readAndDecodeFile(filepath string, handleEntity func(oce *OfflineCacheEntity)) error {
	r, err := mmap.Open(filepath) // open mmap reader
	if err != nil {
		return fmt.Errorf("error opening file <%s> in memory: %w", filepath, err)
	}
	defer r.Close()
	p := make([]byte, r.Len()) // read into byte slice
	if _, err = r.ReadAt(p, 0); err != nil {
		return fmt.Errorf("error reading file <%s> in memory: %w", filepath, err)
	}
	dec := gob.NewDecoder(bufio.NewReader(bytes.NewReader(p)))
	for {
		var oce *OfflineCacheEntity
		if err := dec.Decode(&oce); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode OfflineCacheEntity at <%s>: %w", filepath, err)
		}
		// Call the handler function for each decoded entity
		handleEntity(oce)
	}
	return nil
}

// collect caching items on each set/remove to be dumped to file later on
func (coll *OfflineCollector) collect(itemID string) {
	coll.collMux.Lock()
	coll.collection[itemID] = &CollectionEntity{
		IsSet:  true,
		ItemID: itemID,
	}
	coll.collMux.Unlock()
}

// encodeAndDump OfflineCacheEntity to file
func encodeAndDump(oce *OfflineCacheEntity, enc *gob.Encoder, w *bufio.Writer) (err error) {
	if err = enc.Encode(oce); err != nil {
		return fmt.Errorf("encode error: <%w>", err)
	}
	if err = w.Flush(); err != nil {
		return fmt.Errorf("write error: <%w>", err)
	}
	return
}

// rotateFileIfNeeded checks the size of the file and rotates it if it exceeds the limit. (not thread safe)
func rotateFileIfNeeded(fldrPath string, writeLimit int, file *os.File) (newFile *os.File,
	writer *bufio.Writer, encoder *gob.Encoder, err error) {
	fileStat, err := file.Stat()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error getting file stat: %w", err)
	}
	// if file size excedes write limit in bytes, close the file and create a new one
	// including new writer and encoder
	if fileStat.Size() > int64(writeLimit) {
		var prefix string // give tmpRewriteName prefix to the new file to be created when rewriting files
		if strings.HasPrefix(filepath.Base(file.Name()), tmpRewriteName) {
			prefix = tmpRewriteName
		}
		if err := file.Close(); err != nil {
			return nil, nil, nil, fmt.Errorf("error closing file: %w", err)
		}
		return populateEncoder(fldrPath, prefix)
	}
	return
}

// writeEntity writes SET or REMOVE entity on dump file
func (coll *OfflineCollector) writeEntity(oce *OfflineCacheEntity) error {
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	var err error
	if file, writer, encoder, err := rotateFileIfNeeded(coll.fldrPath,
		coll.writeLimit, coll.file); err != nil {
		return err
	} else if encoder != nil { // if rotateFileIfNeeded encoder returned nil it means rotating files
		//  wasnt needed and didnt happen
		coll.file, coll.writer, coll.encoder = file, writer, encoder
	}
	if err = encodeAndDump(oce, coll.encoder, coll.writer); err != nil {
		coll.logger.Err(fmt.Sprintf("Error <%v>, writing cache item <%#v>", err, oce))
	}
	return err
}

// storeRemoveEntity dumps the removed Cache itemID on file or collects the entity
func (coll *OfflineCollector) storeRemoveEntity(itemID string, dumpInterval time.Duration) {
	if dumpInterval == -1 {
		if err := coll.writeEntity(&OfflineCacheEntity{ItemID: itemID}); err != nil {
			coll.logger.Err(err.Error())
			return
		}
		return
	}
	coll.collMux.Lock()
	coll.collection[itemID] = &CollectionEntity{ItemID: itemID}
	coll.collMux.Unlock()
}

// rewriteFiles will gather all sets and removes from dump files and rewrite a new streamlined dump file (is thread safe)
func (coll *OfflineCollector) rewriteFiles() (err error) {
	coll.rewriteMux.Lock()
	defer coll.rewriteMux.Unlock()
	filePaths, oceMap, skip, err := coll.getFilePathsAndOfflineEntities()
	if skip || err != nil { // make sure rewriting is needed before continuing
		return
	}
	tmpRewritePath := path.Join(coll.fldrPath, tmpRewriteName)   // temporary path to rewrite file
	zeroRewritePath := path.Join(coll.fldrPath, rewriteFileName) // path to completed rewrite file,
	// named 0Rewrite so it stays always first in order of reading files
	oldRewritePath := path.Join(coll.fldrPath, oldRewriteName) // path to old 0Rewrite file renamed to oldRewrite
	// create a new temporary rewrite file, used to populate the serialized dump files collected from coll.fldrPath
	file, err := os.OpenFile(tmpRewritePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	tmpFilePaths := []string{tmpRewritePath} // list of temporary rewritten files paths
	defer func() {
		// delete tmpRewrite files if any errors while rewriting so that we dont try to recover from them
		if err != nil {
			file.Close()
			for i := range tmpFilePaths {
				if rmvErr := os.Remove(tmpFilePaths[i]); rmvErr != nil {
					coll.logger.Warning("Failed to remove tmp rewritten file <" + tmpFilePaths[i] + ">, error: " + rmvErr.Error())
				}
			}
		}
	}()
	writer := bufio.NewWriter(file)
	enc := gob.NewEncoder(writer)
	// range over the streamlined cache items read from dump, and write each one in
	// temporary tmpRewritePath file
	for _, oce := range oceMap {
		if newFile, newWriter, newEnc, err := rotateFileIfNeeded(coll.fldrPath, coll.writeLimit,
			file); err != nil {
			return fmt.Errorf("error rewriting <%w>", err)
		} else if newEnc != nil { // if rotateFileIfNeeded encoder returned nil it means rotating
			// files wasnt needed
			file, writer, enc = newFile, newWriter, newEnc
			// since file size was limited, add the newly created temporary rewrite file path
			//  <newFile.Name> to the tmpFilePaths list
			tmpFilePaths = append(tmpFilePaths, newFile.Name())
		}
		if err := encodeAndDump(oce, enc, writer); err != nil {
			coll.logger.Warning(fmt.Sprintf("Rewrite failed. OfflineCacheEntity <%#v> \nError <%v>", oce, err))
			return err
		}
	}
	file.Close()
	// Rename old 0Rewrite files to oldRewrite if they exist
	for i := range filePaths {
		if strings.Contains(filePaths[i], zeroRewritePath) {
			if err = os.Rename(filePaths[i], oldRewritePath+strconv.Itoa(i)); err != nil {
				return fmt.Errorf("failed to rename file from <%s>, to <%s>, error <%w>",
					zeroRewritePath, oldRewritePath+strconv.Itoa(i), err)
			}
			filePaths[i] = oldRewritePath + strconv.Itoa(i)
		}
	}
	// Rename TMPRewrite to 0Rewrite when all dump files are rewritten succesfuly and old 0Rewrite files are renamed to oldRewrite
	for i := range tmpFilePaths {
		// rename so that we can keep the order but also make it unique from rewrite to rewrite to avoid accidental deleting
		// account for a maximum of digit number of iterations so we keep the order of the files
		index := fmt.Sprintf(fmt.Sprintf("%%0%dd", len(strconv.Itoa(len(tmpFilePaths)))), i)
		zeroRPath := zeroRewritePath + index
		if err = os.Rename(tmpFilePaths[i], zeroRPath); err != nil {
			return fmt.Errorf("failed to rename file from <%s> to <%s>, error <%w> ",
				tmpFilePaths[i], zeroRPath, err)
		}
	}
	for i := range filePaths { // remove old redundant files after everything was successful
		if err = os.Remove(filePaths[i]); err != nil {
			return fmt.Errorf("failed to remove file <%s>, error <%w> ", filePaths[i], err)
		}
	}
	return nil
}

// getFilePathsAndOfflineEntities will look into the cache dump folder and return the
// paths to each file inside it, excluding current opened dump file. Returns also the streamlined cache
// dump it read from all the files gathered
func (coll *OfflineCollector) getFilePathsAndOfflineEntities() (filePaths []string,
	oceMap map[string]*OfflineCacheEntity, skip bool, err error) {
	coll.fileMux.RLock() // make sure current opened dump file isnt switched while cache
	//  dump folder is being read
	currentDumpFilePath := coll.file.Name() // save path of file which is currently being
	// used to dump live cache, so that we can skip rewriting it
	// Walk the directory to collect file paths
	if err := filepath.WalkDir(coll.fldrPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Exclude root and current dump file paths from filePaths
		if !d.IsDir() && !strings.HasSuffix(currentDumpFilePath, d.Name()) {
			filePaths = append(filePaths, path)
		}
		return nil
	}); err != nil {
		coll.fileMux.RUnlock()
		return nil, nil, false, fmt.Errorf("error <%w> walking path <%v>", err, coll.fldrPath)
	}
	coll.fileMux.RUnlock()

	if shouldSkipRewrite(filePaths, coll.fldrPath) {
		return nil, nil, true, nil
	}
	oceMap = make(map[string]*OfflineCacheEntity)   // momentarily hold only necessary entities of all files of cache dump. Needed so we don’t write something which will be removed on the next coming files.
	handleEntity := func(oce *OfflineCacheEntity) { // will add/delete OfflineCacheEntity from oceMap
		if oce.IsSet {
			oceMap[oce.ItemID] = oce
		} else {
			delete(oceMap, oce.ItemID)
		}
	}
	for i := range filePaths { // populate oceMap from dump files
		if err := readAndDecodeFile(filePaths[i], handleEntity); err != nil {
			return nil, nil, false, fmt.Errorf("error <%w> reading file <%v>", err, filePaths[i])
		}
	}
	return
}

// shouldSkipRewrite will return true to skip a rewrite if no dumpfiles are found on filePaths.
// Otherwise means dumpfiles should be rewritten without touching the current open dump file.
func shouldSkipRewrite(filePaths []string, cacheFldrPath string) bool {
	for _, fileName := range filePaths {
		if !strings.HasPrefix(fileName, path.Join(cacheFldrPath, rewriteFileName)) {
			return false // non rewrite file found, dont skip rewriting
		}
	}
	return true
}
