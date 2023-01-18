package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/patrickmn/go-cache"
	"go.uber.org/zap"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
)

type CacheService interface {
	Set(key string, value interface{}, d time.Duration) error
	Get(key string) (interface{}, time.Time, error)
	SaveFile() error
	LoadFile() error
	Updated() bool
}

type MarshalFn func(p interface{}) (interface{}, error)

type cacheService struct {
	dataDir   string
	cache     *cache.Cache
	logger    logger.AppLogger
	marshalFn MarshalFn
	loadedAt  int64
	updatedAt int64
}

func NewCacheService(dataDir string, logger logger.AppLogger, marshalFn MarshalFn) (*cacheService, error) {
	if dataDir == "" || logger == nil {
		return nil, errors.NewAppError(errors.ERROR_MISSING_REQUIRED)
	}

	default_expiration := 5 * time.Minute
	cleanup_interval := 10 * time.Minute
	c := cache.New(default_expiration, cleanup_interval)

	cacheService := &cacheService{
		dataDir:   dataDir,
		cache:     c,
		logger:    logger,
		marshalFn: marshalFn,
	}

	err := cacheService.LoadFile()
	if err != nil {
		logger.Info("starting with fresh cache")
	}

	return cacheService, nil
}

func (c *cacheService) Set(key string, value interface{}, d time.Duration) error {
	err := c.cache.Add(key, value, d)
	if err != nil {
		c.logger.Error(ERROR_SET_CACHE, zap.Error(err), zap.String("cacheDir", c.dataDir))
		return errors.WrapError(err, ERROR_SET_CACHE)
	}
	c.updatedAt = time.Now().Unix()
	c.logger.Debug(VALUE_ADDED, zap.String("key", key), zap.String("cacheDir", c.dataDir))
	return nil
}

func (c *cacheService) Get(key string) (interface{}, time.Time, error) {
	val, exp, ok := c.cache.GetWithExpiration(key)
	if !ok {
		c.logger.Error(ERROR_GET_CACHE, zap.Error(ErrGetCache), zap.String("cacheDir", c.dataDir))
		return nil, time.Time{}, ErrGetCache
	}
	c.logger.Debug(RETURNING_VALUE, zap.String("key", key), zap.String("cacheDir", c.dataDir))
	return val, exp, nil
}

func (c *cacheService) SaveFile() error {
	filePath := filepath.Join(c.dataDir, fmt.Sprintf("%s.json", CACHE_FILE_NAME))
	c.logger.Info("saving cache file", zap.String("filePath", filePath))

	_, err := os.Stat(filepath.Dir(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
		}
	}
	if err != nil {
		c.logger.Error(ERROR_CREATING_CACHE_DIR, zap.Error(err), zap.String("filePath", filePath))
		return ErrSaveCacheFile
	}

	file, err := os.Create(filePath)
	if err != nil {
		c.logger.Error(ERROR_GETTING_CACHE_FILE, zap.Error(err), zap.String("filePath", filePath))
		return ErrGetCacheFile
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	items := c.cache.Items()
	err = encoder.Encode(items)
	if err != nil {
		c.logger.Error(ERROR_SAVING_CACHE_FILE, zap.Error(err), zap.String("filePath", filePath))
		return ErrSaveCacheFile
	}
	c.logger.Info("cache file saved", zap.String("filePath", filePath))
	return nil
}

func (c *cacheService) LoadFile() error {
	filePath := filepath.Join(c.dataDir, fmt.Sprintf("%s.json", CACHE_FILE_NAME))
	c.logger.Info("loading cache file", zap.String("filePath", filePath))
	file, err := os.Open(filePath)
	if err != nil {
		c.logger.Error(ERROR_OPENING_CACHE_FILE, zap.Error(err))
		return err
	}

	err = c.load(file)
	if err != nil {
		c.logger.Error(ERROR_LOADING_CACHE_FILE, zap.Error(err), zap.String("filePath", filePath))
		return err
	}
	return nil
}

func (c *cacheService) Updated() bool {
	c.logger.Info("cache file", zap.Int64("loadedAt", c.loadedAt), zap.Int64("updatedAt", c.updatedAt))
	return c.updatedAt > c.loadedAt
}

func (c *cacheService) load(r io.Reader) error {
	dec := json.NewDecoder(r)
	items := map[string]cache.Item{}
	err := dec.Decode(&items)
	if err == nil {
		for k, v := range items {
			if !v.Expired() {
				obj, err := c.marshalFn(v.Object)
				if err != nil {
					c.logger.Error("error marshalling file object", zap.Error(err), zap.String("cacheDir", c.dataDir))
				} else {
					err = c.Set(k, obj, 5*time.Hour)
					if err != nil {
						c.logger.Error(ERROR_SET_CACHE, zap.Error(err), zap.String("cacheDir", c.dataDir))
					} else {
						c.logger.Debug("cache item loaded", zap.String("cacheDir", c.dataDir), zap.String("key", k), zap.Any("value", obj), zap.Any("exp", v.Expiration))
					}
				}
			}
		}
	}
	c.setLoadedAt(time.Now().Unix())
	c.logger.Info("cache file loaded", zap.Int64("loadedAt", c.loadedAt), zap.Int64("updatedAt", c.updatedAt))
	return err
}

func (c *cacheService) setLoadedAt(at int64) {
	c.loadedAt = at
	c.updatedAt = at
}

func (c *cacheService) delete(key string) {
	c.cache.Delete(key)
	c.updatedAt = time.Now().Unix()
	c.logger.Debug(KEY_DELETED, zap.String("key", key), zap.String("cacheDir", c.dataDir))
}

func (c *cacheService) deleteExpired() {
	c.cache.DeleteExpired()
	c.logger.Debug(DELETED_EXPIRED, zap.String("cacheDir", c.dataDir))
}

func (c *cacheService) itemCount() int {
	count := c.cache.ItemCount()
	c.logger.Info(RETURNING_COUNT, zap.String("cacheDir", c.dataDir))
	return count
}

func (c *cacheService) items() map[string]cache.Item {
	items := c.cache.Items()
	c.logger.Info(RETURNING_ALL_ITEMS, zap.String("cacheDir", c.dataDir))
	return items
}

func (c *cacheService) clear() {
	c.cache.Flush()
	c.logger.Info(CACHE_FLUSHED, zap.String("cacheDir", c.dataDir))
}
