package cache_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/cache"
	"github.com/comfforts/logger"
)

const TEST_DIR = "data"

type TestStruct struct {
	Name string
	Age  int
}

func UnmarshallTestStruct(p interface{}) (interface{}, error) {
	var st TestStruct
	body, err := json.Marshal(p)
	if err != nil {
		return st, err
	}

	err = json.Unmarshal(body, &st)
	if err != nil {
		return st, err
	}
	return st, nil
}

func TestCache(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client cache.CacheService,
	){
		"cache set get, succeeds":        testSetGet,
		"cache set get delete, succeeds": testSetGetDelete,
		"cache set expire, succeeds":     testSetGetExpire,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, teardown := setupTest(t)
			defer teardown()
			fn(t, client)
		})
	}
}

func setupTest(t *testing.T) (
	ca cache.CacheService,
	teardown func(),
) {
	t.Helper()

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = TEST_DIR
	}

	testLogger := logger.NewTestAppLogger(dataDir)
	cacheCfg := cache.CacheConfig{
		DataDir:   dataDir,
		MarshalFn: UnmarshallTestStruct,
	}

	// ca, err := cache.NewCacheService(cacheCfg, testLogger)
	// require.NoError(t, err)

	var err error
	credsPath := os.Getenv("CREDS_PATH")
	bktName := os.Getenv("BUCKET_NAME")
	if credsPath != "" && bktName != "" {
		cloudCfg := cache.CacheStorageConfig{
			CredsPath: credsPath,
			Bucket:    bktName,
		}
		ca, err = cache.NewWithCloudBackup(cacheCfg, cloudCfg, testLogger)
		require.NoError(t, err)
	} else {
		ca, err = cache.NewCacheService(cacheCfg, testLogger)
		require.NoError(t, err)
	}
	require.Equal(t, true, ca != nil)

	return ca, func() {
		t.Log(" TestCache ended")

		err = ca.Clear()
		require.NoError(t, err)
	}
}

func testSetGet(t *testing.T, ca cache.CacheService) {
	val := TestStruct{
		Name: "John",
		Age:  34,
	}
	key := "test"

	now := time.Now().Unix()
	err := ca.Set(key, val, 5*time.Minute)
	require.NoError(t, err)

	count := ca.ItemCount()
	require.Equal(t, 1, count)

	cVal, exp := ca.Get(key)
	require.Equal(t, int64(300), exp.Unix()-now)

	rVal, ok := cVal.(TestStruct)
	require.Equal(t, true, ok)
	require.Equal(t, val.Age, rVal.Age)
	require.Equal(t, val.Name, rVal.Name)
}

func testSetGetDelete(t *testing.T, ca cache.CacheService) {
	val := TestStruct{
		Name: "John",
		Age:  34,
	}
	key := "test"

	now := time.Now().Unix()
	err := ca.Set(key, val, 5*time.Minute)
	require.NoError(t, err)

	cVal, exp := ca.Get(key)
	require.Equal(t, int64(300), exp.Unix()-now)

	rVal, ok := cVal.(TestStruct)
	require.Equal(t, true, ok)
	require.Equal(t, val.Age, rVal.Age)
	require.Equal(t, val.Name, rVal.Name)

	count := ca.ItemCount()
	require.Equal(t, 1, count)

	ca.Delete(key)

	count = ca.ItemCount()
	require.Equal(t, 0, count)
}

func testSetGetExpire(t *testing.T, ca cache.CacheService) {
	val := TestStruct{
		Name: "John",
		Age:  34,
	}
	key := "test"

	now := time.Now().Unix()
	err := ca.Set(key, val, 3*time.Second)
	require.NoError(t, err)

	cVal, exp := ca.Get(key)
	require.Equal(t, int64(3), exp.Unix()-now)

	rVal, ok := cVal.(TestStruct)
	require.Equal(t, true, ok)
	require.Equal(t, val.Age, rVal.Age)
	require.Equal(t, val.Name, rVal.Name)

	count := ca.ItemCount()
	require.Equal(t, 1, count)

	time.Sleep(3 * time.Second)

	ca.DeleteExpired()

	count = ca.ItemCount()
	require.Equal(t, 0, count)
}

func TestSetGetReload(t *testing.T) {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = TEST_DIR
	}

	testLogger := logger.NewTestAppLogger(dataDir)
	cacheCfg := cache.CacheConfig{
		DataDir:       dataDir,
		CacheFileName: "delivery",
		MarshalFn:     UnmarshallTestStruct,
	}
	ca, err := cache.NewCacheService(cacheCfg, testLogger)
	require.NoError(t, err)

	val := TestStruct{
		Name: "John",
		Age:  34,
	}
	key := "test"

	now := time.Now().Unix()
	err = ca.Set(key, val, 5*time.Minute)
	require.NoError(t, err)

	cVal, exp := ca.Get(key)
	require.Equal(t, int64(300), exp.Unix()-now)

	rVal, ok := cVal.(TestStruct)
	require.Equal(t, true, ok)
	require.Equal(t, val.Age, rVal.Age)
	require.Equal(t, val.Name, rVal.Name)

	count := ca.ItemCount()
	require.Equal(t, 1, count)

	updated := ca.Updated()
	require.Equal(t, true, updated)

	err = ca.Clear()
	require.NoError(t, err)

	ca, err = cache.NewCacheService(cacheCfg, testLogger)
	require.NoError(t, err)

	count = ca.ItemCount()
	require.Equal(t, 1, count)

	updated = ca.Updated()
	require.Equal(t, false, updated)

	err = ca.ClearFile()
	require.NoError(t, err)
}

func TestSetGetReloadCloud(t *testing.T) {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = TEST_DIR
	}

	credsPath := os.Getenv("CREDS_PATH")
	bktName := os.Getenv("BUCKET_NAME")
	require.Equal(t, true, credsPath != "")
	require.Equal(t, true, bktName != "")

	testLogger := logger.NewTestAppLogger(dataDir)
	cacheCfg := cache.CacheConfig{
		DataDir:   dataDir,
		MarshalFn: UnmarshallTestStruct,
	}

	cloudCfg := cache.CacheStorageConfig{
		CredsPath: credsPath,
		Bucket:    bktName,
	}
	ca, err := cache.NewWithCloudBackup(cacheCfg, cloudCfg, testLogger)
	require.NoError(t, err)

	val := TestStruct{
		Name: "Shiminic",
		Age:  43,
	}
	key := "test10"

	now := time.Now().Unix()
	err = ca.Set(key, val, 5*time.Minute)
	require.NoError(t, err)

	cVal, exp := ca.Get(key)
	require.Equal(t, int64(300), exp.Unix()-now)

	rVal, ok := cVal.(TestStruct)
	require.Equal(t, true, ok)
	require.Equal(t, val.Age, rVal.Age)
	require.Equal(t, val.Name, rVal.Name)

	time.Sleep(50 * time.Millisecond)

	count := ca.ItemCount()
	require.Equal(t, 1, count)

	err = ca.Clear()
	require.NoError(t, err)

	ca, err = cache.NewWithCloudBackup(cacheCfg, cloudCfg, testLogger)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	count = ca.ItemCount()
	require.Equal(t, 1, count)

	err = ca.ClearFile()
	require.NoError(t, err)
}
