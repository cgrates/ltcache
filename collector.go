/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"bufio"
	"bytes"
	"container/list"
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
	rewriteFileName = "0Rewrite"   // prefix of the name used for files that have been rewritten, starting with 0 to give natural directory walking priority
	tmpRewriteName  = "tmpRewrite" // prefix of the name of files which are in the process of being rewritten
	oldRewriteName  = "oldRewrite" // prefix of the name of files to be deleted after renewing rewrite files
)

// Used to temporarily hold caching instances, until dumped to file
type OfflineCollector struct {
	setCollMux      map[string]*sync.RWMutex                  // lock setColl per cachingInstance so that we dont dump while modifying them
	remCollMux      sync.RWMutex                              // used to lock remColl map
	allCollMux      sync.RWMutex                              // used to lock all of the setColl map
	rewriteMux      sync.RWMutex                              // lock rewriting process
	fileMux         sync.RWMutex                              // used to lock the maps of files, writers and encoders, so we dont have concurrency while writing/reading
	setColl         map[string]map[string]*OfflineCacheEntity // map[cachingInstance]map[cacheItemKey]*item   Collects all key-values SET on cache
	remColl         map[string][]string                       // map[cachingInstance][]cacheItemKey Collects all keys to be removed from files and setColl
	folderPath      string                                    // path to the database dump folder
	dumpInterval    time.Duration                             // holds duration to wait until next dump
	rewriteInterval time.Duration                             // holds duration to wait until next rewrite
	files           map[string]*os.File                       // holds the files opened
	writers         map[string]*bufio.Writer                  // holds the buffer writers per caching instance, used to flush after writing
	encoders        map[string]*gob.Encoder                   // holds encoder per caching instance
	writeLimit      int                                       // maximum size in MiB that can be written in a singular dump file
	logger          logger

	stopWriting    chan struct{} // Used to stop inverval writing
	writeStopped   chan struct{} // signal when writing is finished
	stopRewrite    chan struct{} // Used to stop inverval rewrite
	rewriteStopped chan struct{} // signal when writing is finished
}

// Used to temporarily hold cache items in memory, until dumped to file
type OfflineCacheEntity struct {
	IsSet      bool      // Controls if the item that is written is a set or a remove of the item
	CacheID    string    // Holds the cache ID of the item to be stored in file
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

// Create Directories if they dont exist
func ensureDir(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return err
}

// open/create dump file, connect an encoder to it and store that encoder to the OfflineCollector
func (coll *OfflineCollector) populateEncoders(chInstance string) error {
	filePath := filepath.Join(coll.folderPath, chInstance, strconv.FormatInt(time.Now().UnixMilli(), 10)) // path of the dump file of current caching instance, in miliseconds in case a rewrite happens withing a second of a dump file created
	var err error
	coll.files[chInstance], err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	coll.writers[chInstance] = bufio.NewWriter(coll.files[chInstance])
	coll.encoders[chInstance] = gob.NewEncoder(coll.writers[chInstance])
	return nil
}

// Read dump files per caching instance and recover to new transCache
func processDumpFiles(chInstance, fldrPath string, maxItems int, ttl time.Duration, staticTTL bool, tc *TransCache, tcCacheMux *sync.RWMutex) error {
	offEntity := make(map[string]*OfflineCacheEntity) // struct to decode to from file [cacheID]cacheData
	filePaths, err := getFilePaths(path.Join(fldrPath, chInstance))
	if err != nil {
		return fmt.Errorf("error walking the path: %w", err)
	}
	paths, err := validateFilePaths(filePaths, path.Join(fldrPath, chInstance))
	if err != nil {
		return err
	}
	for _, filepath := range paths {
		if err := readAndDecodeFile(filepath, offEntity); err != nil {
			return err
		}
	}
	tcCacheMux.Lock()
	// Populate TransCache Caches
	tc.cache[chInstance] = newCacheFromDump(offEntity, maxItems, ttl, staticTTL,
		func(itemID string, _ any) { tc.offCollector.storeRemoveEntity(chInstance, itemID) })
	tcCacheMux.Unlock()
	return nil
}

// make sure we dont recover from files that were stopped mid way rewriting
func validateFilePaths(paths []string, prefix string) (validPaths []string, err error) {
	// if there are paths with "oldRewrite" prefix, recover from them instead of 0Rewrite
	// having an oldRewrite still in the tree means the rewriting process was interupted
	var removeZeroRewrite bool
	for _, s := range paths {
		if strings.HasPrefix(s, path.Join(prefix, oldRewriteName)) {
			removeZeroRewrite = true
			break
		}
	}
	for _, s := range paths {
		// dont include "tmpRewrite" paths
		if strings.HasPrefix(s, path.Join(prefix, tmpRewriteName)) {
			if err := os.Remove(s); err != nil {
				return nil, err
			}
			continue
		}
		if removeZeroRewrite && strings.HasPrefix(s, path.Join(prefix, rewriteFileName)) {
			if err := os.Remove(s); err != nil {
				return nil, err
			}
			continue
		}
		validPaths = append(validPaths, s)
	}
	return
}

// WalkDir and get all file paths on that directory
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

// Read dump file and decode
func readAndDecodeFile(filepath string, offEntity map[string]*OfflineCacheEntity) error {
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
		// If the decoded OfflineCacheEntity is a set command populate offEntity
		if oce.IsSet {
			offEntity[oce.CacheID] = oce
		} else { // if its a remove command, delete it from offEntity
			delete(offEntity, oce.CacheID)
		}
	}
	return nil
}

// Method to populate cachedItem with values of recovered OfflineCacheEntity
func (oce *OfflineCacheEntity) toCachedItem() *cachedItem {
	return &cachedItem{
		itemID:     oce.CacheID,
		value:      oce.Value,
		expiryTime: oce.ExpiryTime,
		groupIDs:   oce.GroupIDs,
	}
}

// set adds lru/ttl indexes and refs for each cachedItem. Used only for recovering from dump (not thread safe)
func (c *Cache) set(chID string, cItem *cachedItem) {
	if c.maxEntries == DisabledCaching {
		return
	}
	if c.maxEntries != UnlimitedCaching {
		c.lruRefs[chID] = c.lruIdx.PushFront(cItem)
	}
	if c.ttl > 0 {
		c.ttlRefs[chID] = c.ttlIdx.PushFront(cItem)
	}
	if c.maxEntries != UnlimitedCaching {
		var lElm *list.Element
		if c.lruIdx.Len() > c.maxEntries {
			lElm = c.lruIdx.Back()
		}
		if lElm != nil {
			c.remove(lElm.Value.(*cachedItem).itemID)
		}
	}
}

// Populate Cache with values of recovered cache
func newCacheFromDump(offEntity map[string]*OfflineCacheEntity, maxEntries int, ttl time.Duration, staticTTL bool,
	onEvicted func(itmID string, value interface{})) *Cache {
	cache := &Cache{
		cache:      make(map[string]*cachedItem),
		groups:     make(map[string]map[string]struct{}),
		onEvicted:  onEvicted,
		maxEntries: maxEntries,
		ttl:        ttl,
		staticTTL:  staticTTL,
		lruIdx:     list.New(),
		lruRefs:    make(map[string]*list.Element),
		ttlIdx:     list.New(),
		ttlRefs:    make(map[string]*list.Element),
	}
	for chID, item := range offEntity {
		cache.cache[chID] = item.toCachedItem()
		cache.set(chID, cache.cache[chID])
		cache.addItemToGroups(chID, item.GroupIDs)
	}
	if cache.ttl > 0 {
		go cache.cleanExpired()
	}
	return cache
}

// Collects caching instances on each set/remove to be dumped to file later on
func (coll *OfflineCollector) collect(cacheInstance, cacheID string) {
	coll.allCollMux.Lock()
	defer coll.allCollMux.Unlock()
	if coll.setColl[cacheInstance] == nil {
		coll.setColl[cacheInstance] = make(map[string]*OfflineCacheEntity)
	}
	coll.setCollMux[cacheInstance].Lock()
	coll.setColl[cacheInstance][cacheID] = &OfflineCacheEntity{
		IsSet:   true,
		CacheID: cacheID,
	}
	coll.setCollMux[cacheInstance].Unlock()
}

// Clears a cache instance from the cache OfflineCollector and deletes its correlating dump files
func (coll *OfflineCollector) clearOfflineInstance(cacheInstance string) {
	// clear setColl and remColl collected in memory
	coll.setCollMux[cacheInstance].Lock()
	defer coll.setCollMux[cacheInstance].Unlock()
	coll.allCollMux.Lock()
	coll.setColl[cacheInstance] = make(map[string]*OfflineCacheEntity)
	coll.allCollMux.Unlock()
	coll.remCollMux.Lock()
	coll.remColl[cacheInstance] = make([]string, 0)
	coll.remCollMux.Unlock()
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	coll.files[cacheInstance].Close()
	// remove the dump files if they exist
	if filePaths, err := getFilePaths(path.Join(coll.folderPath, cacheInstance)); err != nil {
		coll.logger.Err("Error walking the path: " + err.Error())
		return
	} else {
		for i := range filePaths {
			if err := os.Remove(filePaths[i]); err != nil {
				coll.logger.Err("Error removeing the file <" + filePaths[i] + ">: " + err.Error())
			}
		}
	}
	if err := coll.populateEncoders(cacheInstance); err != nil {
		coll.logger.Err(err.Error())
		return
	}
}

// populates OfflineCacheEntity, encodes it, and writes it to file
func encodeAndWrite(oce OfflineCacheEntity, enc *gob.Encoder, w *bufio.Writer) error {
	if err := enc.Encode(&oce); err != nil {
		return fmt.Errorf("encode error: <%w>", err)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("write error: <%w>", err)
	}
	return nil
}

// Will write remove entities to file correlating to the cacheIDs set on remColl
func (coll *OfflineCollector) writeRemoveEntity(chInstance string) error {
	coll.remCollMux.Lock()
	defer coll.remCollMux.Unlock()
	for _, key := range coll.remColl[chInstance] {
		if err := coll.checkAndRotateFile(chInstance); err != nil {
			return err
		}
		coll.fileMux.Lock()
		if err := encodeAndWrite(OfflineCacheEntity{
			IsSet:   false,
			CacheID: key,
		}, coll.encoders[chInstance], coll.writers[chInstance]); err != nil {
			coll.fileMux.Unlock()
			return fmt.Errorf("failed to encode or write cache item Remove for <%s> cacheID <%s>: %w", chInstance, key, err)
		}
		coll.fileMux.Unlock()
	}
	delete(coll.remColl, chInstance) // clear remove collection
	return nil
}

// checkAndRotateFile checks the size of the file and rotates it if it exceeds the limit.
func (coll *OfflineCollector) checkAndRotateFile(chInstance string) error {
	if coll.writeLimit == -1 {
		return nil
	}
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	fileStat, err := coll.files[chInstance].Stat()
	if err != nil {
		return fmt.Errorf("error getting file stat: %w", err)
	}
	if fileStat.Size() > int64(coll.writeLimit)*1024*1024 {
		if err := coll.files[chInstance].Close(); err != nil {
			return fmt.Errorf("error closing file: %w", err)
		}
		if err := coll.populateEncoders(chInstance); err != nil {
			return err
		}
	}
	return nil
}

// Decides weather to write the cache on file instantly or put it in the collector to store in intervals
func (coll *OfflineCollector) storeCache(chInstance, cacheID string, value any,
	expiryTime time.Time, groupIDs []string) (err error) {
	if coll.dumpInterval == 0 {
		return
	}
	if coll.dumpInterval == -1 {
		coll.setCollMux[chInstance].Lock()
		defer coll.setCollMux[chInstance].Unlock()
		return coll.storeSetEntity(chInstance, cacheID, value, expiryTime, groupIDs)
	}
	coll.collect(chInstance, cacheID)
	return
}

// Writes the SET Cache on file
func (coll *OfflineCollector) storeSetEntity(chInstance, cacheID string, value any,
	expiryTime time.Time, groupIDs []string) error {
	if err := coll.checkAndRotateFile(chInstance); err != nil {
		return err
	}
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	if err := encodeAndWrite(OfflineCacheEntity{
		IsSet:      true,
		CacheID:    cacheID,
		Value:      value,
		GroupIDs:   groupIDs,
		ExpiryTime: expiryTime,
	}, coll.encoders[chInstance], coll.writers[chInstance]); err != nil {
		coll.logger.Err("Failed to encode or write cache item for <" + chInstance + ">: " + err.Error())
		return err
	}
	return nil
}

// Writes the REMOVE Cache ID on file or collects REMOVE entities
func (coll *OfflineCollector) storeRemoveEntity(cachingInstance, cacheID string) {
	if coll.dumpInterval == -1 {
		if err := coll.checkAndRotateFile(cachingInstance); err != nil {
			coll.logger.Err(err.Error())
			return
		}
		coll.fileMux.Lock()
		defer coll.fileMux.Unlock()
		if err := encodeAndWrite(OfflineCacheEntity{
			IsSet:   false,
			CacheID: cacheID,
		}, coll.encoders[cachingInstance], coll.writers[cachingInstance]); err != nil {
			coll.logger.Err("Failed to encode or write RemoveEntity for <" + cachingInstance + "> cacheID <" +
				cacheID + ">: " + err.Error())
			return
		}
		return
	}
	coll.setCollMux[cachingInstance].Lock()
	delete(coll.setColl[cachingInstance], cacheID)
	coll.setCollMux[cachingInstance].Unlock()
	coll.remCollMux.Lock()
	coll.remColl[cachingInstance] = append(coll.remColl[cachingInstance], cacheID)
	coll.remCollMux.Unlock()
}

// Close opened file and delete if empty
func closeFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting stats for file <%s>: %w", file.Name(), err)

	}
	file.Close()
	if info.Size() == 0 { // if file isnt populated, delete it
		if err := os.Remove(file.Name()); err != nil {
			return fmt.Errorf("error removing file <%s>: %w", file.Name(), err)
		}
	}
	return nil
}

// Rewrite dump files on every rewriteInterval
func (coll *OfflineCollector) runRewrite() {
	if coll.rewriteInterval <= 0 {
		close(coll.rewriteStopped)
		return
	}
	for {
		select {
		case <-coll.stopRewrite: // in case engine is shutdown before interval, dont wait for it
			close(coll.rewriteStopped)
			return
		case <-time.After(coll.rewriteInterval): // no need to instantly rewrite right after reading from files
			coll.rewrite()
		}
	}
}

// Will gather all sets and removes, from dump files and rewrite a new streamlined dump file
func (coll *OfflineCollector) rewrite() {
	coll.rewriteMux.Lock()
	defer coll.rewriteMux.Unlock()

	folders, err := os.ReadDir(coll.folderPath)
	if err != nil {
		coll.logger.Err("Failed to read dir <" + coll.folderPath + "> error: " + err.Error())
		return
	}
	// Iterate through each folder (caching instance folder)
	for _, folder := range folders {
		// Construct the full path of the subdirectory
		subFolderPath := path.Join(coll.folderPath, folder.Name())
		var filePaths []string // each file inside a subdir (caching instance)
		if err := filepath.WalkDir(subFolderPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() { // exclude root path from filepaths.
				filePaths = append(filePaths, path)
			}
			return nil
		}); err != nil {
			coll.logger.Err("Rewriting error walking the path <" + subFolderPath + ">: " + err.Error())
			return
		}
		if coll.shouldContinue(folder.Name(), filePaths, subFolderPath) {
			continue
		}
		offEntity := make(map[string]*OfflineCacheEntity)
		var err error
		for i := range filePaths {
			if err = readAndDecodeFile(filePaths[i], offEntity); err != nil {
				coll.logger.Err(err.Error())
				break
			}
		}
		if err != nil {
			continue
		}
		tmpRewritePath := path.Join(subFolderPath, tmpRewriteName)   // temporary path to rewrite file
		zeroRewritePath := path.Join(subFolderPath, rewriteFileName) // path to completed rewrite file, named 0Rewrite so it stays always first in order of reading files
		oldRewritePath := path.Join(subFolderPath, oldRewriteName)   // path to old 0Rewrite file renamed to oldRewrite
		file, err := os.OpenFile(tmpRewritePath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			coll.logger.Err("Error opening file <" + tmpRewritePath + ">: " + err.Error())
			return
		}
		tmpFilePaths := []string{tmpRewritePath}
		defer func() { // delete tmpRewrite if any errors while rewriting so that we dont try to recover from it
			if err != nil {
				file.Close()
				for i := range tmpFilePaths {
					if err := os.Remove(tmpFilePaths[i]); err != nil {
						coll.logger.Err("Failed to remove tmp rewritten file <" + tmpFilePaths[i] + ">, error: " + err.Error())
					}
				}
			}
		}()
		writer := bufio.NewWriter(file)
		enc := gob.NewEncoder(writer)
		for _, cc := range offEntity {
			if coll.writeLimit > 0 {
				fileStat, _ := file.Stat()
				if fileStat.Size() > int64(coll.writeLimit)*1024*1024 {
					if err := file.Close(); err != nil {
						coll.logger.Err("Error closing file: " + err.Error())
						return
					}
					filePath := tmpRewritePath + strconv.FormatInt(time.Now().UnixMilli(), 10)
					tmpFilePaths = append(tmpFilePaths, filePath)
					file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						coll.logger.Err("Error opening file <" + filePath + ">: " + err.Error())
						return
					}
					writer = bufio.NewWriter(file)
					enc = gob.NewEncoder(writer)
				}
			}
			if err := enc.Encode(cc); err != nil {
				coll.logger.Err(fmt.Sprintf("Rewrite failed to encode cache item %+v: %v", cc, err))
				return
			}
			if err := writer.Flush(); err != nil {
				coll.logger.Err(fmt.Sprintf("Rewrite failed to flush writer for %s: %v", cc.CacheID, err))
				return
			}
		}
		file.Close()
		// Rename old 0Rewrite to oldRewrite if exists
		for i := range filePaths {
			if strings.Contains(filePaths[i], zeroRewritePath) {
				if err = os.Rename(filePaths[i], oldRewritePath+strconv.Itoa(i)); err != nil {
					coll.logger.Err("Failed to rename file from <" + zeroRewritePath + "> to <" + oldRewritePath + strconv.Itoa(i) + ">: " + err.Error())
					return
				}
				filePaths[i] = oldRewritePath + strconv.Itoa(i)
			}
		}
		// Rename TMPRewrite to 0Rewrite
		for i := range tmpFilePaths {
			// rename so that we can keep the order but also make it unique from rewrite to rewrite to avoid accidental deleting
			index := fmt.Sprintf(fmt.Sprintf("%%0%dd", len(strconv.Itoa(len(tmpFilePaths)))), i) // account for a maximum of digit number of iterations so we keep the order of the files
			zeroRPath := zeroRewritePath + index + "_" + strconv.FormatInt(time.Now().UnixMilli(), 10)
			if err = os.Rename(tmpFilePaths[i], zeroRPath); err != nil {
				coll.logger.Err("Failed to rename file from <" + tmpFilePaths[i] + "> to <" + zeroRPath + ">: " + err.Error())
				return
			}
		}
		for i := range filePaths { // remove files included in 0Rewrite
			if err := os.Remove(filePaths[i]); err != nil {
				coll.logger.Err("Failed to remove file <" + filePaths[i] + ">, error: " + err.Error())
			}
		}
	}
}

// used to control skipping a rewrite or not
func (coll *OfflineCollector) shouldContinue(chInstance string, filePaths []string, subFolderPath string) bool {
	coll.fileMux.Lock()
	defer coll.fileMux.Unlock()
	fileStat, _ := coll.files[chInstance].Stat() // Get stat of dump file in current use
	var nonRewriteFiles int
	for _, fileName := range filePaths {
		if !strings.HasPrefix(fileName, path.Join(subFolderPath, rewriteFileName)) {
			nonRewriteFiles++
			if nonRewriteFiles == 2 { // rewrite if new dump file is populated
				break
			}
		}
	}
	// there will always be at least 1 non rewriten file when engine is open
	if nonRewriteFiles == 1 && fileStat.Size() == 0 { // dont rewrite if dump file isnt populated
		return true
	}
	// Close current open file so that we can rewrite it
	if err := coll.files[chInstance].Close(); err != nil {
		coll.logger.Err("error closing file <" + coll.files[chInstance].Name() + ">: " + err.Error())
		return true // dont rewrite if errored
	}
	// Open a new file to continue to write
	if err := coll.populateEncoders(chInstance); err != nil {
		coll.logger.Err(err.Error())
		return true // dont rewrite if errored
	}
	return false
}
