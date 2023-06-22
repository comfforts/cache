package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/patrickmn/go-cache"
	"go.uber.org/zap"

	"github.com/comfforts/cloudstorage"
	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
)

const (
	DEFAULT_CACHE_FILE_NAME  = "cache"
	DEFAULT_EXPIRATION       = 5 * time.Minute
	DEFAULT_CLEANUP_INTERVAL = 10 * time.Minute
)

type CacheService interface {
	Set(key string, value interface{}, d time.Duration) error
	Get(key string) (interface{}, time.Time)
	Delete(key string)
	DeleteExpired()
	ItemCount() int
	Items() map[string]cache.Item
	Updated() bool
	Clear() error
	ClearFile() error
}

type CacheConfig struct {
	DataDir       string
	CacheFileName string
	MarshalFn
	DefaultExpiration      time.Duration
	DefaultCleanupInterval time.Duration
}

type CacheStorageConfig struct {
	CredsPath   string
	Bucket      string
	CloudClient cloudstorage.CloudStorage
}

type MarshalFn func(p interface{}) (interface{}, error)

type cacheService struct {
	CacheConfig
	loadedAt  int64
	updatedAt int64
	cache     *cache.Cache
	logger.AppLogger
	StoreConfig CacheStorageConfig
}

func newCacheService(cfg CacheConfig, l logger.AppLogger) (*cacheService, error) {
	if cfg.DataDir == "" || l == nil {
		return nil, errors.NewAppError(errors.ERROR_MISSING_REQUIRED)
	}

	if cfg.MarshalFn == nil {
		return nil, errors.NewAppError("missing cache data marshalling function")
	}

	defaultExp := cfg.DefaultExpiration
	if defaultExp <= 0 {
		defaultExp = DEFAULT_EXPIRATION
	}
	cleanupInterval := cfg.DefaultCleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = DEFAULT_CLEANUP_INTERVAL
	}

	if cfg.CacheFileName == "" {
		cfg.CacheFileName = DEFAULT_CACHE_FILE_NAME
	}

	c := cache.New(defaultExp, cleanupInterval)

	cacheService := &cacheService{
		CacheConfig: cfg,
		cache:       c,
		AppLogger:   l,
	}
	return cacheService, nil
}

func NewCacheService(cfg CacheConfig, l logger.AppLogger) (*cacheService, error) {
	cacheService, err := newCacheService(cfg, l)
	if err != nil {
		return nil, err
	}

	err = cacheService.loadFile()
	if err != nil {
		l.Info("starting with fresh cache")
	}

	return cacheService, nil
}

func NewWithCloudBackup(cacheCfg CacheConfig, cloudCfg CacheStorageConfig, l logger.AppLogger) (*cacheService, error) {
	if cacheCfg.DataDir == "" || l == nil {
		return nil, errors.NewAppError(errors.ERROR_MISSING_REQUIRED)
	}

	if cacheCfg.MarshalFn == nil {
		return nil, errors.NewAppError("missing cache data marshalling function")
	}

	if cloudCfg.CloudClient == nil {
		if cloudCfg.Bucket == "" || cloudCfg.CredsPath == "" {
			l.Error("missing bucket and cloud credentials")
			return nil, errors.NewAppError("missing bucket and cloud credentials")
		}

		cscCfg := cloudstorage.CloudStorageClientConfig{
			CredsPath: cloudCfg.CredsPath,
		}
		csc, err := cloudstorage.NewCloudStorageClient(cscCfg, l)
		if err != nil {
			l.Error("error creating cloud storage client", zap.Error(err))
			return nil, errors.NewAppError("error creating cloud storage client")
		}
		cloudCfg.CloudClient = csc
	}
	if cloudCfg.Bucket == "" {
		l.Error("missing bucket information")
		return nil, errors.NewAppError("missing bucket information")
	}

	ca, err := newCacheService(cacheCfg, l)
	if err != nil {
		return nil, err
	}
	ca.StoreConfig = cloudCfg

	err = ca.loadFile()
	if err != nil {
		l.Info("starting with fresh cache")
	}

	return ca, nil
}

func (c *cacheService) Set(key string, value interface{}, d time.Duration) error {
	err := c.cache.Add(key, value, d)
	if err != nil {
		c.Error("error setting cache", zap.Error(err), zap.String("key", key), zap.Any("value", value))
		return errors.WrapError(err, ERROR_SET_CACHE)
	}
	c.updatedAt = time.Now().Unix()
	return nil
}

func (c *cacheService) Get(key string) (interface{}, time.Time) {
	val, exp, ok := c.cache.GetWithExpiration(key)
	if !ok {
		return nil, exp
	}
	return val, exp
}

func (c *cacheService) Delete(key string) {
	c.delete(key)
}

func (c *cacheService) DeleteExpired() {
	c.deleteExpired()
}

func (c *cacheService) Items() map[string]cache.Item {
	return c.items()
}

func (c *cacheService) ItemCount() int {
	return c.itemCount()
}

func (c *cacheService) Clear() error {
	return c.clear()
}

func (c *cacheService) ClearFile() error {
	filePath := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	c.Info("removing cache file", zap.String("filePath", filePath))
	_, err := os.Stat(filePath)
	if err != nil {
		c.Error("error accessing file", zap.Error(err), zap.String("filePath", filePath))
		return errors.WrapError(err, "error accessing file %s", filePath)
	}

	var cloudErr error
	if c.StoreConfig.CloudClient != nil {
		cloudErr = c.deleteCloudCache()
		if cloudErr != nil {
			c.Error("error deleting cloud cache file")
		}
	}

	err = os.Remove(filePath)
	if err != nil {
		c.Error("error removing file", zap.Error(err), zap.String("filePath", filePath))
		return errors.WrapError(err, "error removing file %s", filePath)
	}
	return cloudErr
}

func (c *cacheService) Updated() bool {
	c.Info("cache file status", zap.Int64("loadedAt", c.loadedAt), zap.Int64("updatedAt", c.updatedAt))
	return c.updatedAt > c.loadedAt
}

func (c *cacheService) loadFile() error {
	filePath := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	c.Info("loading cache file", zap.String("filePath", filePath))

	_, err := os.Stat(filePath)
	if err != nil {
		if c.StoreConfig.CloudClient != nil {
			err := c.downloadCloudCache()
			if err != nil {
				c.Error("error getting cache file from storage")
				return errors.WrapError(err, "error getting cache file from storage")
			}
		} else {
			c.Error("error no cache file")
			return errors.WrapError(err, "error no cache file")
		}
	}

	file, err := os.Open(filePath)
	defer func() {
		err := file.Close()
		if err != nil {
			c.Error("error closing file after loading", zap.Error(err))
		}
	}()
	if err != nil {
		return errors.WrapError(err, ERROR_OPENING_CACHE_FILE)
	}

	err = c.load(file)
	if err != nil {
		return errors.WrapError(err, ERROR_LOADING_CACHE_FILE)
	}
	return nil
}

func (c *cacheService) load(r io.Reader) error {
	dec := json.NewDecoder(r)
	items := map[string]cache.Item{}
	err := dec.Decode(&items)
	if err == nil {
		for k, v := range items {
			if !v.Expired() {
				obj, err := c.MarshalFn(v.Object)
				if err != nil {
					c.Error("error marshalling file object", zap.Error(err), zap.String("cacheDir", c.DataDir))
				} else {
					err = c.Set(k, obj, 5*time.Hour)
					if err != nil {
						c.Error(ERROR_SET_CACHE, zap.Error(err), zap.String("cacheDir", c.DataDir))
					} else {
						c.Debug("cache item loaded", zap.String("cacheDir", c.DataDir), zap.String("key", k), zap.Any("value", obj), zap.Any("exp", v.Expiration))
					}
				}
			}
		}
	}
	c.setLoadedAt(time.Now().Unix())
	c.Info("cache file loaded", zap.Int64("loadedAt", c.loadedAt), zap.Int64("updatedAt", c.updatedAt))
	return err
}

func (c *cacheService) setLoadedAt(at int64) {
	c.loadedAt = at
	c.updatedAt = at
}

func (c *cacheService) delete(key string) {
	c.cache.Delete(key)
	c.updatedAt = time.Now().Unix()
	c.Debug(KEY_DELETED, zap.String("key", key), zap.String("cacheDir", c.DataDir))
}

func (c *cacheService) deleteExpired() {
	c.cache.DeleteExpired()
	c.Debug(DELETED_EXPIRED, zap.String("cacheDir", c.DataDir))
}

func (c *cacheService) itemCount() int {
	count := c.cache.ItemCount()
	c.Info(RETURNING_COUNT, zap.String("cacheDir", c.DataDir))
	return count
}

func (c *cacheService) items() map[string]cache.Item {
	items := c.cache.Items()
	c.Info(RETURNING_ALL_ITEMS, zap.String("cacheDir", c.DataDir))
	return items
}

func (c *cacheService) clear() error {
	if c.Updated() {
		c.Info("cleaning up geo code data structures")
		err := c.saveFile()
		if err != nil {
			c.Error("error saving cache file", zap.Error(err))
			return err
		}

		if c.StoreConfig.CloudClient != nil {
			err = c.uploadCloudCache()
			if err != nil {
				c.Error("error uploading cache file", zap.Error(err))
				return err
			}
		}
	}

	c.cache.Flush()
	c.Info(CACHE_FLUSHED, zap.String("cacheDir", c.DataDir))

	if c.StoreConfig.CloudClient != nil {
		err := c.StoreConfig.CloudClient.Close()
		if err != nil {
			c.Error("error closing cloud storage client", zap.Error(err))
			return err
		}
	}

	return nil
}

func (c *cacheService) saveFile() error {
	filePath := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	c.Info("saving cache file", zap.String("filePath", filePath))

	_, err := os.Stat(filepath.Dir(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
		}
	}
	if err != nil {
		return errors.WrapError(err, ERROR_CREATING_CACHE_DIR)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return errors.WrapError(err, ERROR_GETTING_CACHE_FILE)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			c.Error("error closing file after saving", zap.Error(err))
		}
	}()

	encoder := json.NewEncoder(file)
	items := c.cache.Items()
	err = encoder.Encode(items)
	if err != nil {
		return errors.WrapError(err, ERROR_SAVING_CACHE_FILE)
	}
	c.Info("cache file saved", zap.String("filePath", filePath))
	return nil
}

func (c *cacheService) deleteCloudCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if c.StoreConfig.CloudClient == nil {
		c.Error("missing cloud storage client")
		return errors.NewAppError("missing cloud storage client")
	}

	cacheFile := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	fStats, err := os.Stat(cacheFile)
	if err != nil {
		c.Error("error accessing file", zap.Error(err), zap.String("filepath", cacheFile))
		return errors.WrapError(err, "error accessing file %s", cacheFile)
	}

	fmod := fStats.ModTime().Unix()
	c.Info("file mod time", zap.Int64("modtime", fmod), zap.String("filepath", cacheFile))

	cfr, err := cloudstorage.NewCloudFileRequest(
		c.StoreConfig.Bucket,
		filepath.Base(cacheFile),
		filepath.Dir(cacheFile),
		fmod,
	)
	if err != nil {
		c.Error("error creating cloud file request", zap.Error(err), zap.String("filepath", cacheFile))
		return err
	}

	err = c.StoreConfig.CloudClient.DeleteObject(ctx, cfr)
	if err != nil {
		c.Error("error deleting cloud file", zap.Error(err))
		return err
	}
	return nil
}

func (c *cacheService) uploadCloudCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if c.StoreConfig.CloudClient == nil {
		c.Error("missing cloud storage client")
		return errors.NewAppError("missing cloud storage client")
	}

	cacheFile := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	fStats, err := os.Stat(cacheFile)
	if err != nil {
		c.Error("error accessing file", zap.Error(err), zap.String("filepath", cacheFile))
		return errors.WrapError(err, "error accessing file %s", cacheFile)
	}

	fmod := fStats.ModTime().Unix()
	c.Info("file mod time", zap.Int64("modtime", fmod), zap.String("filepath", cacheFile))

	file, err := os.Open(cacheFile)
	if err != nil {
		c.Error("error accessing file", zap.Error(err), zap.String("filepath", cacheFile))
		return errors.WrapError(err, "error opening file %s", cacheFile)
	}
	defer func() {
		if err := file.Close(); err != nil {
			c.Error("error closing file", zap.Error(err), zap.String("filepath", cacheFile))
		}
	}()

	cfr, err := cloudstorage.NewCloudFileRequest(
		c.StoreConfig.Bucket,
		filepath.Base(cacheFile),
		filepath.Dir(cacheFile),
		fmod,
	)
	if err != nil {
		c.Error("error creating file upload request", zap.Error(err), zap.String("filepath", cacheFile))
		return err
	}

	n, err := c.StoreConfig.CloudClient.UploadFile(ctx, file, cfr)
	if err != nil {
		c.Error("error uploading file", zap.Error(err))
		return err
	}
	c.Info("uploaded file",
		zap.String("file", filepath.Base(cacheFile)),
		zap.String("path", filepath.Dir(cacheFile)),
		zap.Int64("bytes", n),
	)
	return nil
}

func (c *cacheService) downloadCloudCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if c.StoreConfig.CloudClient == nil {
		c.Error("missing cloud storage client")
		return errors.NewAppError("missing cloud storage client")
	}

	cacheFile := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	fStats, err := os.Stat(cacheFile)
	var fmod int64
	if err != nil {
		err = os.MkdirAll(filepath.Dir(cacheFile), os.ModePerm)
		if err != nil {
			c.Error("error creating file directory", zap.Error(err), zap.String("filepath", cacheFile))
			return errors.WrapError(err, "error creating file directory")
		}
	} else {
		fmod = fStats.ModTime().Unix()
		c.Info("file mod time", zap.Int64("modtime", fmod), zap.String("filepath", cacheFile))
	}

	f, err := os.Create(cacheFile)
	if err != nil {
		c.Error("error creating file", zap.Error(err), zap.String("filepath", cacheFile))
		return errors.WrapError(err, "error creating file %s", cacheFile)
	}
	defer func() {
		if err := f.Close(); err != nil {
			c.Error("error closing file", zap.Error(err), zap.String("filepath", cacheFile))
		}
	}()

	cfr, err := cloudstorage.NewCloudFileRequest(
		c.StoreConfig.Bucket,
		filepath.Base(cacheFile),
		filepath.Dir(cacheFile),
		fmod,
	)
	if err != nil {
		c.Error("error creating cloud upload request", zap.Error(err), zap.String("filepath", cacheFile))
		return err
	}

	n, err := c.StoreConfig.CloudClient.DownloadFile(ctx, f, cfr)
	if err != nil {
		c.Error("error downloading file", zap.Error(err), zap.String("filepath", cacheFile))
		return err
	}
	c.Info(
		"downloaded file",
		zap.String("file", filepath.Base(cacheFile)),
		zap.String("path", filepath.Dir(cacheFile)),
		zap.Int64("bytes", n))
	return nil
}
