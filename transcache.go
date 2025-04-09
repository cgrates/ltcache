/*
TransCache is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM GmbH. All Rights Reserved.

TransCache is a bigger version of Cache with support for multiple Cache instances and transactions
*/

package ltcache

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

const (
	AddItem              = "AddItem"
	RemoveItem           = "RemoveItem"
	RemoveGroup          = "RemoveGroup"
	DefaultCacheInstance = "*default"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNotClonable = errors.New("not clonable")
)

func GenUUID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] &^ 0x40) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10],
		b[10:])
}

// CacheCloner is an interface for objects to clone themselves into interface
type CacheCloner interface {
	CacheClone() any
}

type transactionItem struct {
	verb     string      // action which will be executed on cache
	cacheID  string      // cache instance identifier
	itemID   string      // item itentifier
	value    interface{} // item value
	groupIDs []string    // attach item to groups
}

type CacheConfig struct {
	MaxItems  int
	TTL       time.Duration
	StaticTTL bool
	OnEvicted []func(itmID string, value interface{})
	Clone     bool
}

// NewTransCache instantiates a new TransCache
func NewTransCache(cfg map[string]*CacheConfig) (tc *TransCache) {
	if _, has := cfg[DefaultCacheInstance]; !has { // Default always created
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc = &TransCache{
		cache:             make(map[string]*Cache),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem),
	}
	for cacheID, chCfg := range cfg {
		tc.cache[cacheID] = NewCache(chCfg.MaxItems, chCfg.TTL, chCfg.StaticTTL, chCfg.Clone, chCfg.OnEvicted)
	}
	return
}

// TransCache is a bigger cache with transactions and multiple Cache instances support
type TransCache struct {
	cache    map[string]*Cache       // map[cacheInstance]cacheStore
	cfg      map[string]*CacheConfig // map[cacheInstance]*CacheConfig
	cacheMux sync.RWMutex            // so we can apply the complete transaction buffer in one shoot

	transactionBuffer map[string][]*transactionItem // Queue tasks based on transactionID
	transBufMux       sync.Mutex                    // Protects the transactionBuffer
	transactionMux    sync.Mutex                    // Queue transactions on commit
}

// cacheInstance returns a specific cache instance based on ID or default
func (tc *TransCache) cacheInstance(chID string) (c *Cache) {
	var ok bool
	if c, ok = tc.cache[chID]; !ok {
		c = tc.cache[DefaultCacheInstance]
	}
	return
}

// BeginTransaction initializes a new transaction into transactions buffer
func (tc *TransCache) BeginTransaction() (transID string) {
	transID = GenUUID()
	tc.transBufMux.Lock()
	tc.transactionBuffer[transID] = make([]*transactionItem, 0)
	tc.transBufMux.Unlock()
	return transID
}

// RollbackTransaction destroys a transaction from transactions buffer
func (tc *TransCache) RollbackTransaction(transID string) {
	tc.transBufMux.Lock()
	delete(tc.transactionBuffer, transID)
	tc.transBufMux.Unlock()
}

// CommitTransaction executes the actions in a transaction buffer
func (tc *TransCache) CommitTransaction(transID string) {
	tc.transactionMux.Lock()
	tc.transBufMux.Lock()
	tc.cacheMux.Lock() // apply all transactioned items in one shot
	for _, item := range tc.transactionBuffer[transID] {
		switch item.verb {
		case AddItem:
			tc.Set(item.cacheID, item.itemID, item.value, item.groupIDs, true, transID)
		case RemoveItem:
			tc.Remove(item.cacheID, item.itemID, true, transID)
		case RemoveGroup:
			if len(item.groupIDs) >= 1 {
				tc.RemoveGroup(item.cacheID, item.groupIDs[0], true, transID)
			}
		}
	}
	tc.cacheMux.Unlock()
	delete(tc.transactionBuffer, transID)
	tc.transBufMux.Unlock()
	tc.transactionMux.Unlock()
}

// Get returns the value of an Item
func (tc *TransCache) Get(chID, itmID string) (interface{}, bool) {
	tc.cacheMux.RLock()
	defer tc.cacheMux.RUnlock()
	return tc.cacheInstance(chID).Get(itmID)
}

// Set will add/edit an item to the cache
func (tc *TransCache) Set(chID, itmID string, value interface{},
	groupIDs []string, commit bool, transID string) {
	if commit {
		if transID == "" { // Lock locally
			tc.cacheMux.Lock()
			defer tc.cacheMux.Unlock()
		}
		tc.cacheInstance(chID).Set(itmID, value, groupIDs)
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID,
				verb: AddItem, itemID: itmID,
				value: value, groupIDs: groupIDs})
		tc.transBufMux.Unlock()
	}
}

// RempveItem removes an item from the cache
func (tc *TransCache) Remove(chID, itmID string, commit bool, transID string) {
	if commit {
		if transID == "" { // Lock per operation not transaction
			tc.cacheMux.Lock()
			defer tc.cacheMux.Unlock()
		}
		tc.cacheInstance(chID).Remove(itmID)
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID, verb: RemoveItem, itemID: itmID})
		tc.transBufMux.Unlock()
	}
}

func (tc *TransCache) HasGroup(chID, grpID string) (has bool) {
	tc.cacheMux.RLock()
	has = tc.cacheInstance(chID).HasGroup(grpID)
	tc.cacheMux.RUnlock()
	return
}

// GetGroupItems returns all items in a group. Nil if group does not exist
func (tc *TransCache) GetGroupItemIDs(chID, grpID string) (itmIDs []string) {
	tc.cacheMux.RLock()
	itmIDs = tc.cacheInstance(chID).GetGroupItemIDs(grpID)
	tc.cacheMux.RUnlock()
	return
}

// GetGroupItems returns all items in a group. Nil if group does not exist
func (tc *TransCache) GetGroupItems(chID, grpID string) (itms []interface{}) {
	tc.cacheMux.RLock()
	itms = tc.cacheInstance(chID).GetGroupItems(grpID)
	tc.cacheMux.RUnlock()
	return
}

// RemoveGroup removes a group of items out of cache
func (tc *TransCache) RemoveGroup(chID, grpID string, commit bool, transID string) {
	if commit {
		if transID == "" { // Lock locally
			tc.cacheMux.Lock()
			defer tc.cacheMux.Unlock()
		}
		tc.cacheInstance(chID).RemoveGroup(grpID)
	} else {
		tc.transBufMux.Lock()
		tc.transactionBuffer[transID] = append(tc.transactionBuffer[transID],
			&transactionItem{cacheID: chID, verb: RemoveGroup, groupIDs: []string{grpID}})
		tc.transBufMux.Unlock()
	}
}

// Remove all items in one or more cache instances
func (tc *TransCache) Clear(chIDs []string) {
	tc.cacheMux.Lock()
	if chIDs == nil {
		chIDs = make([]string, len(tc.cache))
		i := 0
		for chID := range tc.cache {
			chIDs[i] = chID
			i += 1
		}
	}
	for _, chID := range chIDs {
		tc.cacheInstance(chID).Clear()
	}
	tc.cacheMux.Unlock()
}

// GetItemIDs returns a list of item IDs matching prefix
func (tc *TransCache) GetItemIDs(chID, prfx string) (itmIDs []string) {
	tc.cacheMux.RLock()
	itmIDs = tc.cacheInstance(chID).GetItemIDs(prfx)
	tc.cacheMux.RUnlock()
	return
}

// GetItemExpiryTime returns the expiry time of an item, ok is false if not found
func (tc *TransCache) GetItemExpiryTime(chID, itmID string) (exp time.Time, ok bool) {
	tc.cacheMux.RLock()
	defer tc.cacheMux.RUnlock()
	return tc.cacheInstance(chID).GetItemExpiryTime(itmID)
}

// HasItem verifies if Item is in the cache
func (tc *TransCache) HasItem(chID, itmID string) (has bool) {
	tc.cacheMux.RLock()
	has = tc.cacheInstance(chID).HasItem(itmID)
	tc.cacheMux.RUnlock()
	return
}

// GetCacheStats returns on overview of full cache
func (tc *TransCache) GetCacheStats(chIDs []string) (cs map[string]*CacheStats) {
	cs = make(map[string]*CacheStats)
	tc.cacheMux.RLock()
	if len(chIDs) == 0 {
		for chID := range tc.cache {
			chIDs = append(chIDs, chID)
		}
	}
	for _, chID := range chIDs {
		cs[chID] = tc.cacheInstance(chID).GetCacheStats()
	}
	tc.cacheMux.RUnlock()
	return
}

// TransCacheOpts holds the options needed to create a TransCache with OfflineCollector
type TransCacheOpts struct {
	DumpPath        string        // path where TransCache will be dumped
	BackupPath      string        // path where dump files will backup
	StartTimeout    time.Duration // if time to start TransCache passes this duration, it will stop and return error
	DumpInterval    time.Duration // dump frequency interval at which cache will be dumped to file (-1 dumps cache as soon as a set/remove is done; 0 disables it)
	RewriteInterval time.Duration // rewrite the dump files to streamline them, using RewriteInterval. (-2 rewrites on shutdown, -1 rewrites before start of dumping, 0 disables it).
	FileSizeLimit   int           // File size limit in bytes. When limit is passed, it creates a new file where cache will be dumped. (only bigger than 0 allowed)
}

// NewTransCacheWithOfflineCollector constructs a new TransCache with OfflineCollector if opts are
// provided. If not it runs NewTransCache constructor. Cache configuration is taken from cfg and logs
// will be sent to l logger.
func NewTransCacheWithOfflineCollector(opts *TransCacheOpts, cfg map[string]*CacheConfig, l logger) (tc *TransCache, err error) {
	if opts == nil { // if no opts are provided, create a TransCache without offline collector
		return NewTransCache(cfg), nil
	}
	if opts.FileSizeLimit <= 0 {
		return nil, fmt.Errorf("fileSizeLimit has to be bigger than 0. Current fileSizeLimit <%v> bytes", opts.FileSizeLimit)
	}
	if _, err = os.Stat(opts.DumpPath); err != nil { // ensure directory exists
		return nil, err
	}
	if _, exists := cfg[DefaultCacheInstance]; !exists {
		cfg[DefaultCacheInstance] = &CacheConfig{MaxItems: -1}
	}
	tc = &TransCache{
		cache:             make(map[string]*Cache),
		cfg:               cfg,
		transactionBuffer: make(map[string][]*transactionItem),
	}
	var wg sync.WaitGroup                   // wait for all goroutines to finish reading dump
	errChan := make(chan error, 1)          // signal error from newCacheFromFolder
	constructed := make(chan struct{})      // signal transCache constructed
	for cacheName, config := range tc.cfg { // range over cfg to create each cache and populate TransCache.cache with them
		// Create folder if it doesnt exist
		if err := os.MkdirAll(path.Join(opts.DumpPath, cacheName), 0755); err != nil {
			return nil, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			offColl := NewOfflineCollector(cacheName, opts, l)
			cache, err := NewCacheFromFolder(offColl, config.MaxItems, config.TTL, config.StaticTTL, config.Clone, config.OnEvicted)
			if err != nil {
				errChan <- err
				return
			}
			tc.cacheMux.Lock() // avoid locking all of tc.cache map while the caching instance isn't constructed yet
			tc.cache[cacheName] = cache
			tc.cacheMux.Unlock()
		}()
	}
	go func() { // wait in goroutine for reading from dump to be finished. In cases when an error is returned from newCacheFromFolder, instantly return the error and stop proccessing
		wg.Wait()
		close(constructed)
	}()

	select {
	case <-time.After(opts.StartTimeout):
		return nil, fmt.Errorf("building TransCache from <%s> timed out after <%v>", opts.DumpPath, opts.StartTimeout)
	case err := <-errChan:
		return nil, err
	case <-constructed:
		return
	}
}

// DumpAll collected cache in files
func (tc *TransCache) DumpAll() (err error) {
	var wg sync.WaitGroup
	errChan := make(chan error, len(tc.cache)) // Channel to collect errors
	for cacheKey, cache := range tc.cache {
		if cache.offCollector == nil {
			return fmt.Errorf("couldn't dump cache to file, %s offCollector is nil", cacheKey)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cache.DumpToFile(); err != nil {
				cache.offCollector.logger.Err(err.Error()) // dont stop other caches from dumping if previous DumpToFile errors
				errChan <- err
			}
		}()
	}
	wg.Wait()
	close(errChan) // Close the channel after all goroutines are done
	// Check if there were any errors
	for err = range errChan {
		if err != nil { // Set the first error encountered
			return
		}
	}
	return
}

// RewriteAll will gather all sets and removes from dump files and rewrite a new streamlined file
func (tc *TransCache) RewriteAll() (err error) {
	var wg sync.WaitGroup
	errChan := make(chan error, len(tc.cache)) // Channel to collect errors
	for _, cache := range tc.cache {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cache.RewriteDumpFiles(); err != nil { // dont stop other
				// caches from rewriting if previous RewriteDumpFiles errors
				errChan <- err
			}
		}()
	}
	wg.Wait()
	close(errChan) // Close the channel after all goroutines are done
	// Check if there were any errors
	for err = range errChan {
		if err != nil { // Set the first error encountered
			return
		}
	}
	return
}

// Shutdown depending on dump and rewrite intervals, will dump all thats left in
// cache collector to file and/or rewrite files, and close all files
func (tc *TransCache) Shutdown() {
	for _, c := range tc.cache {
		if c.offCollector == nil {
			return // dont return any error on shutdown where collector was disabled
		}
		if err := c.Shutdown(); err != nil { // only log errors to make sure we dont stop other caches from shutting down
			c.offCollector.logger.Err(err.Error())
		}
	}
}

// BackupDumpFolder will momentarely stop any dumping and rewriting per Cache until their
// dump folder is backed up in folder path backupFolderPath, making zip true will create
// a zip file for each Cache dump in the backupFolderPath instead.
func (tc *TransCache) BackupDumpFolder(backupFolderPath string, zip bool) (err error) {
	newBackupFldrName := "backups_" +
		strconv.FormatInt(time.Now().UnixMilli(), 10) // the name that will be used to
		// create a new backup folder
	var newBackupFldrPath string // create new backup folder in newBackupFldrPath path
	// where each Cache dump will be pasted
	for _, cache := range tc.cache { // lock all dumping or rewriting until function returns
		if cache.offCollector == nil {
			return fmt.Errorf("cache's offCollector is nil")
		}
		if backupFolderPath == "" {
			backupFolderPath = cache.offCollector.backupPath
		}
		if !zip {
			newBackupFldrPath = path.Join(backupFolderPath, newBackupFldrName) // create new backup folder in backupFolderPath path where each Cache dump will be pasted
			if err = os.MkdirAll(newBackupFldrPath, 0755); err != nil {
				return
			}
			if err = cache.BackupCacheDumpFolder(newBackupFldrPath, false); err != nil {
				return
			}
			continue
		}
		if err = cache.BackupCacheDumpFolder(backupFolderPath, true); err != nil {
			return
		}
	}
	return
}
