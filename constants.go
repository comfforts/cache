package cache

import "errors"

const (
	ERROR_MISSING_REQUIRED         string = "missing required parameter"
	ERROR_SET_CACHE                string = "error adding key/value to cache"
	ERROR_GET_CACHE                string = "error getting key/value from cache"
	ERROR_CREATING_CACHE_DIR       string = "error creating cache directory"
	ERROR_GETTING_CACHE_FILE       string = "error getting cache file"
	ERROR_SAVING_CACHE_FILE        string = "error saving cache file"
	ERROR_OPENING_CACHE_FILE       string = "error opening cache file"
	ERROR_LOADING_CACHE_FILE       string = "error loading cache file"
	ERROR_MARSHALLING_CACHE_OBJECT string = "error marshalling object to json"
	ERROR_UNMARSHALLING_CACHE_JSON string = "error unmarshalling json to struct"
	ERROR_MISSING_MARSHALLING_FN   string = "missing cache data marshalling function"
	ERR_MISSING_CLOUD_CRED         string = "missing bucket and cloud credentials"
	ERR_CLOUD_CLIENT_INIT          string = "error initializing cloud storage client"
	ERR_MISSING_BUCKET_NAME        string = "missing cloud storage bucket name"
	MISSING_CLOUD_CLIENT           string = "missing cloud storage client"

	VALUE_ADDED         = "added value to cache"
	RETURNING_VALUE     = "returning value for given key"
	KEY_DELETED         = "deleted value with given key"
	DELETED_EXPIRED     = "deleted expired cache values"
	RETURNING_COUNT     = "returning item count"
	RETURNING_ALL_ITEMS = "returning all items"
	CACHE_FLUSHED       = "cache flushed"
)

var (
	ErrMissingRequired    = errors.New(ERROR_MISSING_REQUIRED)
	ErrSetCache           = errors.New(ERROR_SET_CACHE)
	ErrGetCache           = errors.New(ERROR_GET_CACHE)
	ErrGetCacheFile       = errors.New(ERROR_GETTING_CACHE_FILE)
	ErrSaveCacheFile      = errors.New(ERROR_SAVING_CACHE_FILE)
	ErrMarshallingFn      = errors.New(ERROR_MISSING_MARSHALLING_FN)
	ErrMissingCloudCred   = errors.New(ERR_MISSING_CLOUD_CRED)
	ErrCloudClientInit    = errors.New(ERR_CLOUD_CLIENT_INIT)
	ErrMissingBucket      = errors.New(ERR_MISSING_BUCKET_NAME)
	ErrMissingCloudClient = errors.New(MISSING_CLOUD_CLIENT)
)
