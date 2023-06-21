package cache

import "github.com/comfforts/errors"

const (
	ERROR_SET_CACHE                string = "error adding key/value to cache"
	ERROR_GET_CACHE                string = "error getting key/value from cache"
	ERROR_CREATING_CACHE_DIR       string = "error creating cache directory"
	ERROR_GETTING_CACHE_FILE       string = "error getting cache file"
	ERROR_SAVING_CACHE_FILE        string = "error saving cache file"
	ERROR_OPENING_CACHE_FILE       string = "error opening cache file"
	ERROR_LOADING_CACHE_FILE       string = "error loading cache file"
	ERROR_MARSHALLING_CACHE_OBJECT string = "error marshalling object to json"
	ERROR_UNMARSHALLING_CACHE_JSON string = "error unmarshalling json to struct"

	VALUE_ADDED         = "added value to cache"
	RETURNING_VALUE     = "returning value for given key"
	KEY_DELETED         = "deleted value with given key"
	DELETED_EXPIRED     = "deleted expired cache values"
	RETURNING_COUNT     = "returning item count"
	RETURNING_ALL_ITEMS = "returning all items"
	CACHE_FLUSHED       = "cache flushed"
)

var (
	ErrSetCache      = errors.NewAppError(ERROR_SET_CACHE)
	ErrGetCache      = errors.NewAppError(ERROR_GET_CACHE)
	ErrGetCacheFile  = errors.NewAppError(ERROR_GETTING_CACHE_FILE)
	ErrSaveCacheFile = errors.NewAppError(ERROR_SAVING_CACHE_FILE)
)
