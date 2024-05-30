package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const defaultFileName = "current-data"
const bufferSize = 8192

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out              *os.File
	outPath          string
	outOffset        int64
	directory        string
	segmentSize      int64
	lastSegmentIndex int

	index    hashIndex
	segments []*Segment
}

type Segment struct {
	outOffset int64

	index    hashIndex
	filePath string
}

func NewDatabase(directory string, segmentSize int64) (*Db, error) {
	db := &Db{
		directory:        directory,
		segmentSize:      segmentSize,
		segments:         []*Segment{},
		lastSegmentIndex: 0,
	}

	if err := db.CreateDataSegment(); err != nil {
		return nil, err
	}

	if err := db.Recover(); err != nil && err != io.EOF {
		return nil, err
	}

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
		}

		db.segments = []*Segment{newSegment, db.GetLastDataSegment()}
	}()
}

func IsKeyInNewerSegments(segments []*Segment, key string) bool {
	for _, segment := range segments {
		if _, exists := segment.index[key]; exists {
			return true
		}
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
	lastSegment := db.GetLastDataSegment()
	lastSegment.index[key] = db.outOffset
	db.outOffset += size
}

func (db *Db) GetDataSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		if pos, found := segment.index[key]; found {
			return segment, pos, nil
		}
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
	if segment, position, err := db.GetDataSegmentAndPosition(key); err == nil {
		return segment.GetFromDataSegment(position)
	} else {
		return "", err
	}
}

func (db *Db) Put(key, value string) error {
	entry := entry{
		key:   key,
		value: value,
	}
	entrySize := entry.GetLength()

	fileInfo, err := db.out.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size()+entrySize > db.segmentSize {
		if err := db.CreateDataSegment(); err != nil {
			return err
		}
	}

	bytesWritten, err := db.out.Write(entry.Encode())
	if err != nil {
		return err
	}
	db.SetStorageKey(entry.key, int64(bytesWritten))

	return nil
}
