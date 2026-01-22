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
	"github.com/comfforts/logger"
)

const (
	DEFAULT_CACHE_FILE_NAME  = "cache"
	DEFAULT_EXPIRATION       = 5 * time.Minute
	DEFAULT_CLEANUP_INTERVAL = 10 * time.Minute
)

type CacheService interface {
	Set(ctx context.Context, key string, value any, d time.Duration) error
	Get(key string) (any, time.Time)
	Delete(ctx context.Context, key string)
	DeleteExpired(context.Context)
	ItemCount(context.Context) int
	Items(context.Context) map[string]cache.Item
	Updated(context.Context) bool
	Clear(context.Context) error
	ClearFile(context.Context) error
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

type MarshalFn func(p any) (any, error)

type cacheService struct {
	CacheConfig
	loadedAt    int64
	updatedAt   int64
	cache       *cache.Cache
	StoreConfig CacheStorageConfig
}

func newCacheService(cfg CacheConfig) (*cacheService, error) {
	if cfg.DataDir == "" {
		return nil, ErrMissingRequired
	}

	if cfg.MarshalFn == nil {
		return nil, ErrMarshallingFn
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
	}
	return cacheService, nil
}

func NewCacheService(ctx context.Context, cfg CacheConfig) (*cacheService, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	cacheService, err := newCacheService(cfg)
	if err != nil {
		return nil, err
	}

	err = cacheService.loadFile(ctx)
	if err != nil {
		l.Info("starting with fresh cache")
	}

	return cacheService, nil
}

func NewWithCloudBackup(ctx context.Context, cacheCfg CacheConfig, cloudCfg CacheStorageConfig) (*cacheService, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if cacheCfg.DataDir == "" {
		return nil, ErrMissingRequired
	}

	if cacheCfg.MarshalFn == nil {
		return nil, ErrMarshallingFn
	}

	if cloudCfg.CloudClient == nil {
		if cloudCfg.Bucket == "" || cloudCfg.CredsPath == "" {
			l.Error("missing bucket and cloud credentials")
			return nil, ErrMissingCloudCred
		}

		cscCfg := cloudstorage.CloudStorageClientConfig{
			CredsPath: cloudCfg.CredsPath,
		}
		csc, err := cloudstorage.NewCloudStorageClient(ctx, cscCfg)
		if err != nil {
			l.Error("error creating cloud storage client", zap.Error(err))
			return nil, ErrCloudClientInit
		}
		cloudCfg.CloudClient = csc
	}
	if cloudCfg.Bucket == "" {
		l.Error("missing bucket information")
		return nil, ErrMissingBucket
	}

	ca, err := newCacheService(cacheCfg)
	if err != nil {
		return nil, err
	}
	ca.StoreConfig = cloudCfg

	err = ca.loadFile(ctx)
	if err != nil {
		l.Info("starting with fresh cache")
	}

	return ca, nil
}

func (c *cacheService) Set(ctx context.Context, key string, value any, d time.Duration) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	err = c.cache.Add(key, value, d)
	if err != nil {
		l.Error("error setting cache", "error", err.Error(), "key", key, "value", value)
		return ErrSetCache
	}
	c.updatedAt = time.Now().Unix()
	return nil
}

func (c *cacheService) Get(key string) (any, time.Time) {
	val, exp, ok := c.cache.GetWithExpiration(key)
	if !ok {
		return nil, exp
	}
	return val, exp
}

func (c *cacheService) Delete(ctx context.Context, key string) {
	c.delete(ctx, key)
}

func (c *cacheService) DeleteExpired(ctx context.Context) {
	c.deleteExpired(ctx)
}

func (c *cacheService) Items(ctx context.Context) map[string]cache.Item {
	return c.items(ctx)
}

func (c *cacheService) ItemCount(ctx context.Context) int {
	return c.itemCount(ctx)
}

func (c *cacheService) Clear(ctx context.Context) error {
	return c.clear(ctx)
}

func (c *cacheService) ClearFile(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	filePath := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	l.Debug("removing cache file", "filePath", filePath)
	_, err = os.Stat(filePath)
	if err != nil {
		l.Error("error accessing file", "error", err.Error(), "filePath", filePath)
		return err
	}

	var cloudErr error
	if c.StoreConfig.CloudClient != nil {
		cloudErr = c.deleteCloudCache(ctx)
		if cloudErr != nil {
			l.Error("error deleting cloud cache file")
		}
	}

	err = os.Remove(filePath)
	if err != nil {
		l.Error("error removing file", "error", err.Error(), "filePath", filePath)
		return err
	}
	return cloudErr
}

func (c *cacheService) Updated(ctx context.Context) bool {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	l.Debug("cache file status", "loadedAt", c.loadedAt, "updatedAt", c.updatedAt)
	return c.updatedAt > c.loadedAt
}

func (c *cacheService) loadFile(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	filePath := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	l.Debug("loading cache file", "filePath", filePath)

	_, err = os.Stat(filePath)
	if err != nil {
		if c.StoreConfig.CloudClient != nil {
			err := c.downloadCloudCache(ctx)
			if err != nil {
				l.Error("error getting cache file from storage")
				return err
			}
		} else {
			l.Error("error no cache file")
			return err
		}
	}

	file, err := os.Open(filePath)
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing file after loading", "error", err.Error())
		}
	}()
	if err != nil {
		return err
	}

	err = c.load(ctx, file)
	if err != nil {
		return err
	}
	return nil
}

func (c *cacheService) load(ctx context.Context, r io.Reader) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	dec := json.NewDecoder(r)
	items := map[string]cache.Item{}
	err = dec.Decode(&items)
	if err == nil {
		for k, v := range items {
			if !v.Expired() {
				obj, err := c.MarshalFn(v.Object)
				if err != nil {
					l.Error("error marshalling file object", "error", err.Error(), "cacheDir", c.DataDir)
				} else {
					err = c.Set(ctx, k, obj, 5*time.Hour)
					if err != nil {
						l.Error(ERROR_SET_CACHE, "error", err.Error(), "cacheDir", c.DataDir)
					} else {
						l.Debug("cache item loaded", zap.String("cacheDir", c.DataDir), zap.String("key", k), zap.Any("value", obj), zap.Any("exp", v.Expiration))
					}
				}
			}
		}
	}
	c.setLoadedAt(time.Now().Unix())
	l.Debug("cache file loaded", "loadedAt", c.loadedAt, "updatedAt", c.updatedAt)
	return err
}

func (c *cacheService) setLoadedAt(at int64) {
	c.loadedAt = at
	c.updatedAt = at
}

func (c *cacheService) delete(ctx context.Context, key string) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	c.cache.Delete(key)
	c.updatedAt = time.Now().Unix()
	l.Debug(KEY_DELETED, "key", key, "cacheDir", c.DataDir)
}

func (c *cacheService) deleteExpired(ctx context.Context) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	c.cache.DeleteExpired()
	l.Debug(DELETED_EXPIRED, "cacheDir", c.DataDir)
}

func (c *cacheService) itemCount(ctx context.Context) int {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	count := c.cache.ItemCount()
	l.Debug(RETURNING_COUNT, "cacheDir", c.DataDir)
	return count
}

func (c *cacheService) items(ctx context.Context) map[string]cache.Item {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	items := c.cache.Items()
	l.Debug(RETURNING_ALL_ITEMS, "cacheDir", c.DataDir)
	return items
}

func (c *cacheService) clear(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if c.Updated(ctx) {
		l.Debug("cleaning up geo code data structures")
		err := c.saveFile(ctx)
		if err != nil {
			l.Error("error saving cache file", "error", err.Error())
			return err
		}

		if c.StoreConfig.CloudClient != nil {
			err = c.uploadCloudCache(ctx)
			if err != nil {
				l.Error("error uploading cache file", "error", err.Error())
				return err
			}
		}
	}

	c.cache.Flush()
	l.Debug(CACHE_FLUSHED, "cacheDir", c.DataDir)

	if c.StoreConfig.CloudClient != nil {
		err := c.StoreConfig.CloudClient.Close(ctx)
		if err != nil {
			l.Error("error closing cloud storage client", "error", err.Error())
			return err
		}
	}

	return nil
}

func (c *cacheService) saveFile(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	filePath := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	l.Debug("saving cache file", "filePath", filePath)

	_, err = os.Stat(filepath.Dir(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
		}
	}
	if err != nil {
		l.Error("error creating cache directory", "error", err.Error(), "dirPath", filepath.Dir(filePath))
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		l.Error("error creating cache file", "error", err.Error(), "filePath", filePath)
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.Error("error closing file after saving", "error", err.Error())
		}
	}()

	encoder := json.NewEncoder(file)
	items := c.cache.Items()
	err = encoder.Encode(items)
	if err != nil {
		l.Error("error saving cache file", "error", err.Error(), "filePath", filePath)
		return err
	}
	l.Debug("cache file saved", "filePath", filePath)
	return nil
}

func (c *cacheService) deleteCloudCache(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if c.StoreConfig.CloudClient == nil {
		l.Error("missing cloud storage client")
		return ErrMissingCloudClient
	}

	cacheFile := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	fStats, err := os.Stat(cacheFile)
	if err != nil {
		l.Error("error accessing file", "error", err.Error(), "filepath", cacheFile)
		return err
	}

	fmod := fStats.ModTime().Unix()
	l.Debug("file mod time", "modtime", fmod, "filepath", cacheFile)

	err = c.StoreConfig.CloudClient.DeleteObject(ctx, c.StoreConfig.Bucket, cacheFile)
	if err != nil {
		l.Error("error deleting cloud file", "error", err.Error())
		return err
	}
	return nil
}

func (c *cacheService) uploadCloudCache(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if c.StoreConfig.CloudClient == nil {
		return ErrMissingCloudClient
	}

	cacheFile := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	fStats, err := os.Stat(cacheFile)
	if err != nil {
		l.Error("error accessing file", "error", err.Error(), "filepath", cacheFile)
		return err
	}

	fmod := fStats.ModTime().Unix()
	l.Debug("file mod time", "modtime", fmod, "filepath", cacheFile)

	file, err := os.Open(cacheFile)
	if err != nil {
		l.Error("error accessing file", "error", err.Error(), "filepath", cacheFile)
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			l.Error("error closing file", "error", err.Error(), "filepath", cacheFile)
		}
	}()

	n, err := c.StoreConfig.CloudClient.UploadFile(ctx, file, c.StoreConfig.Bucket, filepath.Dir(cacheFile), filepath.Base(cacheFile))
	if err != nil {
		l.Error("error uploading file", "error", err.Error())
		return err
	}
	l.Debug("uploaded file",
		"file", filepath.Base(cacheFile),
		"path", filepath.Dir(cacheFile),
		"bytes", n,
	)
	return nil
}

func (c *cacheService) downloadCloudCache(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if c.StoreConfig.CloudClient == nil {
		l.Error("missing cloud storage client")
		return ErrMissingCloudClient
	}

	cacheFile := filepath.Join(c.DataDir, fmt.Sprintf("%s.json", c.CacheFileName))
	fStats, err := os.Stat(cacheFile)
	var fmod int64
	if err != nil {
		err = os.MkdirAll(filepath.Dir(cacheFile), os.ModePerm)
		if err != nil {
			l.Error("error creating file directory", "error", err.Error(), "filepath", cacheFile)
			return err
		}
	} else {
		fmod = fStats.ModTime().Unix()
		l.Debug("file mod time", "modtime", fmod, "filepath", cacheFile)
	}

	f, err := os.Create(cacheFile)
	if err != nil {
		l.Error("error creating file", "error", err.Error(), "filepath", cacheFile)
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			l.Error("error closing file", "error", err.Error(), "filepath", cacheFile)
		}
	}()

	n, err := c.StoreConfig.CloudClient.DownloadFile(ctx, f, c.StoreConfig.Bucket, filepath.Dir(cacheFile), filepath.Base(cacheFile))
	if err != nil {
		l.Error("error downloading file", "error", err.Error(), "filepath", cacheFile)
		return err
	}
	l.Debug(
		"downloaded file",
		"file", filepath.Base(cacheFile),
		"path", filepath.Dir(cacheFile),
		"bytes", n)
	return nil
}
