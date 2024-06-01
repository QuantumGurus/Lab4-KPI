package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir, 45)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"1", "v1"},
		{"2", "v2"},
		{"3", "v3"},
	}

	outFile, err := os.Open(filepath.Join(dir, defaultFileName+"0"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDatabase(dir, 45)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	tempDirectory, err := ioutil.TempDir("", "testDBDir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDirectory)

	dbInstance, err := NewDatabase(tempDirectory, 35)
	if err != nil {
		t.Fatal(err)
	}
	defer dbInstance.Close()

	t.Run("verify chunk creation", func(t *testing.T) {
		dbInstance.Put("1", "val1")
		dbInstance.Put("2", "val2")
		dbInstance.Put("3", "val3")
		dbInstance.Put("2", "val5")
		actualSegmentCount := len(dbInstance.segments)
		expectedSegmentCount := 2
		if actualSegmentCount != expectedSegmentCount {
			t.Errorf("Segmentation error. Expected 2 segments, but got %d.", actualSegmentCount)
		}
	})

	t.Run("verify chunk initiation", func(t *testing.T) {
		dbInstance.Put("4", "val4")
		initialSegmentCount := len(dbInstance.segments)
		expectedInitialCount := 3
		if initialSegmentCount != expectedInitialCount {
			t.Errorf("Segmentation error. Expected 3 segments, but got %d.", initialSegmentCount)
		}

		time.Sleep(2 * time.Second)

		finalSegmentCount := len(dbInstance.segments)
		expectedFinalCount := 2
		if finalSegmentCount != expectedFinalCount {
			t.Errorf("Segmentation error. Expected 2 segments after compaction, but got %d.", finalSegmentCount)
		}
	})

	t.Run("verify duplicate recordKey handling", func(t *testing.T) {
		actualValue, _ := dbInstance.Get("2")
		expectedValue := "val5"
		if actualValue != expectedValue {
			t.Errorf("Segmentation error. Expected value: %s, but got: %s", expectedValue, actualValue)
		}
	})

	t.Run("verify chunk file size", func(t *testing.T) {
		file, err := os.Open(dbInstance.segments[0].filePath)
		defer file.Close()

		if err != nil {
			t.Error(err)
		}
		fileInfo, _ := file.Stat()
		actualSize := fileInfo.Size()
		expectedSize := int64(51)
		if actualSize != expectedSize {
			t.Errorf("Segmentation error. Expected size %d, but got %d", expectedSize, actualSize)
		}
	})
}

func TestDb_ConcurrentGets(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir, 45)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Додамо тестові дані
	keysValues := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	for k, v := range keysValues {
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Failed to put %s: %s", k, err)
		}
	}

	// Запустимо паралельні запити
	var wg sync.WaitGroup
	for k, expectedValue := range keysValues {
		wg.Add(1)
		go func(key, expected string) {
			defer wg.Done()
			value, err := db.Get(key)
			if err != nil || value != expected {
				t.Errorf("Failed to get %s: expected %s, got %s (err: %v)", key, expected, value, err)
			}
		}(k, expectedValue)
	}
	wg.Wait()
}
