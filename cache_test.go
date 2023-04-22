package cache_test

import (
	"encoding/json"
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
		"cache set get save, succeeds":   testSetGetSave,
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

	logger := logger.NewTestAppLogger(TEST_DIR)
	ca, err := cache.NewCacheService(TEST_DIR, logger, UnmarshallTestStruct)
	require.NoError(t, err)

	return ca, func() {
		t.Log(" TestCache ended")
		ca.Clear()
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

	cVal, exp, err := ca.Get(key)
	require.NoError(t, err)
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

	cVal, exp, err := ca.Get(key)
	require.NoError(t, err)
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

	cVal, exp, err := ca.Get(key)
	require.NoError(t, err)
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

func testSetGetSave(t *testing.T, ca cache.CacheService) {
	val := TestStruct{
		Name: "John",
		Age:  34,
	}
	key := "test"

	now := time.Now().Unix()
	err := ca.Set(key, val, 5*time.Minute)
	require.NoError(t, err)

	cVal, exp, err := ca.Get(key)
	require.NoError(t, err)
	require.Equal(t, int64(300), exp.Unix()-now)

	rVal, ok := cVal.(TestStruct)
	require.Equal(t, true, ok)
	require.Equal(t, val.Age, rVal.Age)
	require.Equal(t, val.Name, rVal.Name)

	count := ca.ItemCount()
	require.Equal(t, 1, count)

	updated := ca.Updated()
	require.Equal(t, true, updated)

	err = ca.SaveFile()
	require.NoError(t, err)

	ca.Clear()

	count = ca.ItemCount()
	require.Equal(t, 0, count)

	err = ca.LoadFile()
	require.NoError(t, err)

	count = ca.ItemCount()
	require.Equal(t, 1, count)

	updated = ca.Updated()
	require.Equal(t, false, updated)

	err = ca.ClearFile()
	require.NoError(t, err)
}
