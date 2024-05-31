package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const defaultFileName = "current-data"
const bufferSize = 8192

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type IndexAction struct {
	isInsert  bool
	recordKey string
	offset    int64
}

type KeyPosition struct {
	chunk    *Segment
	location int64
}

type EntryWithChan struct {
	entry  entry
	result chan error
}

type Db struct {
	out              *os.File
	outPath          string
	outOffset        int64
	directory        string
	segmentSize      int64
	lastSegmentIndex int
	indexOps         chan IndexAction
	keyPositions     chan *KeyPosition
	putOps           chan EntryWithChan

	segments []*Segment
}

type Segment struct {
	outOffset int64

	index    hashIndex
	filePath string
	mu       sync.Mutex
}

func NewDatabase(directory string, segmentSize int64) (*Db, error) {
	db := &Db{
		directory:        directory,
		segmentSize:      segmentSize,
		segments:         []*Segment{},
		indexOps:         make(chan IndexAction),
		keyPositions:     make(chan *KeyPosition),
		putOps:           make(chan EntryWithChan),
		lastSegmentIndex: 0,
	}

	if err := db.CreateDataSegment(); err != nil {
		return nil, err
	}

	if err := db.Recover(); err != nil && err != io.EOF {
		return nil, err
	}

	db.InitiateIndexProcessor()
	db.InitiateEntryProcessor()

	return db, nil
}

func (db *Db) CreateDataSegment() error {
	filePath := db.GenerateNewFileName()

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}

	db.out = file
	db.outOffset = 0
	db.segments = append(db.segments, newSegment)

	if len(db.segments) >= 3 {
		db.PerformOldSegmentsCompaction()
	}

	return err
}

func (db *Db) GenerateNewFileName() string {
	fileName := fmt.Sprintf("%s%d", defaultFileName, db.lastSegmentIndex)
	filePath := filepath.Join(db.directory, fileName)

	db.lastSegmentIndex++

	return filePath
}

func (db *Db) PerformOldSegmentsCompaction() {
	go func() {
		newFilePath := db.GenerateNewFileName()
		newSegment := &Segment{
			filePath: newFilePath,
			index:    make(hashIndex),
		}

		newFile, err := os.OpenFile(newFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}

		var offset int64

		lastSegmentIdx := len(db.segments) - 2

		for i := 0; i <= lastSegmentIdx; i++ {
			currentSegment := db.segments[i]
			currentSegment.mu.Lock()

			for key, pos := range currentSegment.index {
				if i < lastSegmentIdx && IsKeyInNewerSegments(db.segments[i+1:lastSegmentIdx+1], key) {
					continue
				}

				value, _ := currentSegment.GetFromDataSegment(pos)
				e := entry{
					key:   key,
					value: value,
				}
				n, writeErr := newFile.Write(e.Encode())
				if writeErr == nil {
					newSegment.index[key] = offset
					offset += int64(n)
				}
			}
			currentSegment.mu.Unlock()
		}

		db.segments = []*Segment{newSegment, db.GetLastDataSegment()}
	}()
}

func IsKeyInNewerSegments(segments []*Segment, key string) bool {
	for _, segment := range segments {
		segment.mu.Lock()

		if _, exists := segment.index[key]; exists {
			segment.mu.Unlock()
			return true
		}
		segment.mu.Unlock()
	}
	return false
}

func (db *Db) Recover() error {
	var recoveryErr error
	var dataBuffer [bufferSize]byte

	inputReader := bufio.NewReaderSize(db.out, bufferSize)
	for recoveryErr == nil {
		var (
			headerBytes, dataBytes []byte
			readBytes              int
		)
		headerBytes, recoveryErr = inputReader.Peek(bufferSize)
		if recoveryErr == io.EOF {
			if len(headerBytes) == 0 {
				return recoveryErr
			}
		} else if recoveryErr != nil {
			return recoveryErr
		}
		size := binary.LittleEndian.Uint32(headerBytes)

		if size < bufferSize {
			dataBytes = dataBuffer[:size]
		} else {
			dataBytes = make([]byte, size)
		}
		readBytes, recoveryErr = inputReader.Read(dataBytes)

		if recoveryErr == nil {
			if readBytes != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var recordEntry entry
			recordEntry.Decode(dataBytes)
			db.SetStorageKey(recordEntry.key, int64(readBytes))
		}
	}
	return recoveryErr
}

func (db *Db) SetStorageKey(key string, size int64) {
	db.GetLastDataSegment().mu.Lock()
	defer db.GetLastDataSegment().mu.Unlock()

	lastSegment := db.GetLastDataSegment()
	lastSegment.index[key] = db.outOffset
	db.outOffset += size
}

func (db *Db) GetDataSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		segment.mu.Lock()

		if pos, found := segment.index[key]; found {
			segment.mu.Unlock()
			return segment, pos, nil
		}
		segment.mu.Unlock()
	}
	return nil, 0, ErrNotFound
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) GetLastDataSegment() *Segment {
	lastIndex := len(db.segments) - 1
	return db.segments[lastIndex]
}

func (s *Segment) GetFromDataSegment(position int64) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if _, err := file.Seek(position, 0); err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	return readValue(reader)
}

func (db *Db) Get(key string) (string, error) {
	keyLocation := db.FindKeyPosition(key)
	if keyLocation == nil {
		return "", ErrNotFound
	}
	value, err := keyLocation.chunk.GetFromDataSegment(keyLocation.location)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	result := make(chan error)
	db.putOps <- EntryWithChan{
		entry:  e,
		result: result,
	}
	return <-result
}

func (db *Db) InitiateIndexProcessor() {
	go func() {
		for {
			logEntry := <-db.indexOps
			if logEntry.isInsert {
				db.SetStorageKey(logEntry.recordKey, logEntry.offset)
			} else {
				segment, location, err := db.GetDataSegmentAndPosition(logEntry.recordKey)
				if err != nil {
					db.keyPositions <- nil
				} else {
					db.keyPositions <- &KeyPosition{
						chunk:    segment,
						location: location,
					}
				}
			}
		}
	}()
}

func (db *Db) FindKeyPosition(key string) *KeyPosition {
	action := IndexAction{
		isInsert:  false,
		recordKey: key,
	}
	db.indexOps <- action
	return <-db.keyPositions
}

func (db *Db) InitiateEntryProcessor() {
	go func() {
		for {
			entry := <-db.putOps
			entryLength := entry.entry.GetLength()
			fileInfo, err := db.out.Stat()
			if err != nil {
				entry.result <- err
				continue
			}
			if fileInfo.Size()+entryLength > db.segmentSize {
				err := db.CreateDataSegment()
				if err != nil {
					entry.result <- err
					continue
				}
			}
			bytesWritten, err := db.out.Write(entry.entry.Encode())
			if err == nil {
				db.indexOps <- IndexAction{
					isInsert:  true,
					recordKey: entry.entry.key,
					offset:    int64(bytesWritten),
				}
			}
			entry.result <- nil
		}
	}()
}
