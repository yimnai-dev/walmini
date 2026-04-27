package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-faker/faker/v4"
)

type WAL struct {
	MaxSegmentSize         int
	FileNameLength         int
	RootDir                string
	SegExt                 string
	IndexExt               string
	FilePathPrefix         string
	IndexPathPrefix        string
	WriteRefPath           string
	ReadRefPath            string
	WriteIndexRefPath      string
	ReadIndexRefPath       string
	WriteExt               string
	ReadExt                string
	WriteIndexExt          string
	ReadIndexExt           string
	ReadCheckpointFilePath string
	ReadCheckpointExt      string
}

type WALConfig struct {
	MaxSegmentSize int
	FileNameLength int
	RootDir        string
}

const (
	DEFAULT_MAX_SEGMENT_SIZE    = 2 << 20 // 2 MB
	DEFAULT_SEGMENT_FILE_LENGTH = 9
	DEFAULT_ROOT_DIR            = "../"
	DEFAULT_SEGMENT_EXT         = "seg"
	DEFAULT_WRITE_EXT           = "ref"
	DEFAULT_READ_EXT            = "ref"
	DEFAULT_INDEX_EXT           = "idx"
	DEFAULT_WRITE_INDEX_REF_EXT = "ref"
	DEFAULT_READ_INDEX_REF_EXT  = "ref"
	DEFAULT_READ_CHECKPOINT_EXT = "meta"
	DEFAULT_SEED_SIZE           = 1000
)

func (w *WAL) Init(config WALConfig) {
	if config.FileNameLength == 0 {
		w.FileNameLength = DEFAULT_SEGMENT_FILE_LENGTH
	} else {
		w.FileNameLength = config.FileNameLength
	}
	if config.MaxSegmentSize == 0 {
		w.MaxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE
	} else {
		w.MaxSegmentSize = config.MaxSegmentSize
	}
	if config.RootDir == "" {
		w.RootDir = DEFAULT_ROOT_DIR
	} else {
		w.RootDir = config.RootDir
	}
	w.WriteExt = DEFAULT_WRITE_EXT
	w.ReadExt = DEFAULT_READ_EXT
	w.SegExt = DEFAULT_SEGMENT_EXT
	w.IndexExt = DEFAULT_INDEX_EXT
	w.WriteIndexExt = DEFAULT_WRITE_INDEX_REF_EXT
	w.ReadIndexExt = DEFAULT_READ_INDEX_REF_EXT
	w.ReadCheckpointExt = DEFAULT_READ_CHECKPOINT_EXT
	w.FilePathPrefix = filepath.Join(w.RootDir, "data", "segments")
	w.IndexPathPrefix = filepath.Join(w.RootDir, "data", "indices")
	w.WriteRefPath = filepath.Join(w.RootDir, "data", fmt.Sprintf("write.%s", w.WriteExt))
	w.ReadRefPath = filepath.Join(w.RootDir, "data", fmt.Sprintf("read.%s", w.ReadExt))
	w.WriteIndexRefPath = filepath.Join(w.RootDir, "data", fmt.Sprintf("write_index.%s", w.WriteIndexExt))
	w.ReadIndexRefPath = filepath.Join(w.RootDir, "data", fmt.Sprintf("read_index.%s", w.ReadIndexExt))
	w.ReadCheckpointFilePath = filepath.Join(w.RootDir, "data", fmt.Sprintf("read_checkpoint.%s", w.ReadCheckpointExt))
	errs := w.createDataDirectories()
	if errs != nil && len(*errs) > 0 {
		for _, err := range *errs {
			log.Fatalf("[WAL:SETUP]: Failed to Create Required Data Directory: %s", err)
		}
	}
}

func (w *WAL) createDataDirectories() *[]error {
	var errs []error
	dirs := []string{"segments", "indices"}
	for _, dir := range dirs {
		path := filepath.Join(w.RootDir, "data", dir)
		err := os.MkdirAll(path, os.FileMode(0755))
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return &errs
}

func (w *WAL) openReadCheckpoint() (*os.File, error) {
	file, err := os.OpenFile(w.ReadCheckpointFilePath, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (w *WAL) openWriteRef() (*os.File, error) {
	ref, err := os.OpenFile(w.WriteRefPath, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return ref, nil
}

func (w *WAL) openWriteIndexRef() (*os.File, error) {
	ref, err := os.OpenFile(w.WriteIndexRefPath, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return ref, nil
}

func (w *WAL) openReadRef() (*os.File, error) {
	ref, err := os.OpenFile(w.ReadRefPath, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return ref, nil
}

func (w *WAL) openReadIndexRef() (*os.File, error) {
	ref, err := os.OpenFile(w.ReadIndexRefPath, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return ref, nil
}

func (w *WAL) OpenWriteHead() (*os.File, error) {
	writeRef, err := w.openWriteRef()
	if err != nil {
		return nil, err
	}
	defer writeRef.Close()
	buf, err := io.ReadAll(writeRef)
	if err != nil {
		return nil, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if id == "" {
		id = fmt.Sprintf("%09d", 1)
		err := w.saveWriteHead(id)
		if err != nil {
			return nil, err
		}
	}
	writeHeadPath := filepath.Join(w.FilePathPrefix, fmt.Sprintf("%s.%s", id, w.SegExt))
	head, err := os.OpenFile(writeHeadPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (w *WAL) OpenWriteIndexHead() (*os.File, error) {
	writeRef, err := w.openWriteIndexRef()
	if err != nil {
		return nil, err
	}
	defer writeRef.Close()
	buf, err := io.ReadAll(writeRef)
	if err != nil {
		return nil, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if id == "" {
		id = fmt.Sprintf("%09d", 1)
		err := w.saveWriteIndexHead(id)
		if err != nil {
			return nil, err
		}
	}
	writeHeadPath := filepath.Join(w.IndexPathPrefix, fmt.Sprintf("%s.%s", id, w.IndexExt))
	head, err := os.OpenFile(writeHeadPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (w *WAL) OpenReadHead() (*os.File, error) {
	readRef, err := w.openReadRef()
	if err != nil {
		return nil, err
	}
	defer readRef.Close()
	buf, err := io.ReadAll(readRef)
	if err != nil {
		return nil, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if id == "" {
		id = fmt.Sprintf("%09d", 1)
		err := w.saveReadHead(id)
		if err != nil {
			return nil, err
		}
	}
	readHeadPath := filepath.Join(w.FilePathPrefix, fmt.Sprintf("%s.%s", id, w.SegExt))
	head, err := os.OpenFile(readHeadPath, os.O_CREATE|os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (w *WAL) OpenReadIndexHead() (*os.File, error) {
	readIndexRef, err := w.openReadIndexRef()
	if err != nil {
		return nil, err
	}
	defer readIndexRef.Close()
	buf, err := io.ReadAll(readIndexRef)
	if err != nil {
		return nil, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if id == "" {
		id = fmt.Sprintf("%09d", 1)
		err := w.saveReadIndexHead(id)
		if err != nil {
			return nil, err
		}
	}
	readHeadPath := filepath.Join(w.IndexPathPrefix, fmt.Sprintf("%s.%s", id, w.IndexExt))
	head, err := os.OpenFile(readHeadPath, os.O_CREATE|os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (w *WAL) saveWriteHead(id string) error {
	writeRef, err := w.openWriteRef()
	if err != nil {
		return err
	}
	defer writeRef.Close()
	if err := writeRef.Truncate(0); err != nil {
		return err
	}
	_, err = writeRef.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = writeRef.WriteString(id)
	if err != nil {
		return err
	}
	err = writeRef.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (w *WAL) saveWriteIndexHead(id string) error {
	writeRef, err := w.openWriteIndexRef()
	if err != nil {
		return err
	}
	defer writeRef.Close()
	if err := writeRef.Truncate(0); err != nil {
		return err
	}
	_, err = writeRef.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = writeRef.WriteString(id)
	if err != nil {
		return err
	}
	err = writeRef.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (w *WAL) saveReadHead(id string) error {
	readRef, err := w.openReadRef()
	if err != nil {
		return err
	}
	defer readRef.Close()
	_, err = readRef.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = readRef.WriteString(id)
	if err != nil {
		return err
	}
	err = readRef.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (w *WAL) saveReadIndexHead(id string) error {
	readIndexRef, err := w.openReadIndexRef()
	if err != nil {
		return err
	}
	defer readIndexRef.Close()
	if err = readIndexRef.Truncate(0); err != nil {
		return err
	}
	_, err = readIndexRef.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = readIndexRef.WriteString(id)
	if err != nil {
		return err
	}
	err = readIndexRef.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (w *WAL) formatRecord(record string) []byte {
	recordBuf := []byte(record)
	length := uint32(len(recordBuf))
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, length)
	var buf []byte
	buf = append(buf, lengthBuf...)
	buf = append(buf, recordBuf...)
	return buf
}

func (w *WAL) createSegment() (*os.File, error) {
	ref, err := w.openWriteRef()
	if err != nil {
		return nil, err
	}
	defer ref.Close()
	buf, err := io.ReadAll(ref)
	if err != nil {
		return nil, err
	}
	id := strings.Split(string(buf), "\n")[0]
	idInt, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	if err := ref.Truncate(0); err != nil {
		return nil, err
	}
	nextSegmentName := fmt.Sprintf("%09d", idInt+1)
	err = w.saveWriteHead(nextSegmentName)
	if err != nil {
		return nil, err
	}
	nextSegmentPath := filepath.Join(w.FilePathPrefix, fmt.Sprintf("%s.%s", nextSegmentName, w.SegExt))
	head, err := os.OpenFile(nextSegmentPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (w *WAL) createIndex() (*os.File, error) {
	ref, err := w.openWriteIndexRef()
	if err != nil {
		return nil, err
	}
	defer ref.Close()
	buf, err := io.ReadAll(ref)
	if err != nil {
		return nil, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if err := ref.Truncate(0); err != nil {
		return nil, err
	}
	idInt, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	nextIndexName := fmt.Sprintf("%09d", idInt+1)
	err = w.saveWriteIndexHead(nextIndexName)
	if err != nil {
		return nil, err
	}
	nextIndexPath := filepath.Join(w.IndexPathPrefix, fmt.Sprintf("%s.%s", nextIndexName, w.IndexExt))
	head, err := os.OpenFile(nextIndexPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	return head, nil
}

func (w *WAL) Append(record string) error {
	formattedRecord := w.formatRecord(record)
	formattedRecordLength := len(formattedRecord)
	writeHead, err := w.OpenWriteHead()
	if err != nil {
		return err
	}
	defer writeHead.Close()
	writeIndexHead, err := w.OpenWriteIndexHead()
	if err != nil {
		return err
	}
	defer writeIndexHead.Close()
	stat, err := writeHead.Stat()
	if err != nil {
		return err
	}
	expectedSize := formattedRecordLength + int(stat.Size())
	if expectedSize > w.MaxSegmentSize {
		nextSegment, err := w.createSegment()
		if err != nil {
			return err
		}
		writeHead = nextSegment
		updatedStat, err := writeHead.Stat()
		if err != nil {
			return err
		}
		stat = updatedStat
		nextIndex, err := w.createIndex()
		if err != nil {
			return err
		}
		writeIndexRef, err := w.openWriteIndexRef()
		if err != nil {
			return err
		}
		nextIndexName := regexp.MustCompile(`\d{9}`).FindString(nextIndex.Name())
		if err := writeIndexRef.Truncate(0); err != nil {
			return err
		}
		writeIndexRef.WriteString(nextIndexName)
		writeIndexHead = nextIndex
	}
	_, err = writeHead.Write(formattedRecord)
	if err != nil {
		return err
	}
	err = writeHead.Sync()
	if err != nil {
		return err
	}

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(stat.Size()))
	if _, err = writeIndexHead.Write(buf); err != nil {
		return err
	}
	if err = writeIndexHead.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *WAL) hasNextIndex() (int, error) {
	readIndexRef, err := w.openReadIndexRef()
	if err != nil {
		return 0, err
	}
	defer readIndexRef.Close()
	buf, err := io.ReadAll(readIndexRef)
	if err != nil {
		return 0, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if id == "" {
		id = fmt.Sprintf("%09d", 1)
		err := w.saveReadIndexHead(id)
		if err != nil {
			return 0, err
		}
	}
	n, err := strconv.Atoi(id)
	if err != nil {
		return 0, err
	}
	nextReadIndexName := fmt.Sprintf("%09d.%s", n+1, w.ReadIndexExt)
	if _, err := os.Stat(filepath.Join(w.IndexPathPrefix, nextReadIndexName)); err != nil {
		return 0, err
	}
	return n + 1, nil
}

func (w *WAL) hasPreviousIndex() (int, error) {
	readIndexRef, err := w.openReadIndexRef()
	if err != nil {
		return 0, err
	}
	defer readIndexRef.Close()
	buf, err := io.ReadAll(readIndexRef)
	if err != nil {
		return 0, err
	}
	id := strings.Split(string(buf), "\n")[0]
	if id == "" {
		id = fmt.Sprintf("%09d", 1)
		err := w.saveReadIndexHead(id)
		if err != nil {
			return 0, err
		}
	}
	n, err := strconv.Atoi(id)
	if err != nil {
		return 0, err
	}
	if n-1 < 1 {
		return 0, errors.New("[WAL]: There's no previous Index")
	}
	prevReadIndexName := fmt.Sprintf("%09d.%s", n-1, w.ReadIndexExt)
	if _, err := os.Stat(filepath.Join(w.IndexPathPrefix, prevReadIndexName)); err != nil {
		return 0, err
	}
	return n - 1, nil
}

func (w *WAL) getCheckpoint(checkpointFile *os.File) (uint64, error) {
	checkpointFileStat, err := checkpointFile.Stat()
	if err != nil {
		return 0, err
	}
	if checkpointFileStat.Size() == 0 {
		err := w.saveCheckpoint(checkpointFile, 0)
		if err != nil {
			msg := fmt.Sprintf("[CHECKPOINT] Failed to Initialize Checkpoint: %s", err)
			return 0, errors.New(msg)
		}
		return w.getCheckpoint(checkpointFile)
	}
	if _, err := checkpointFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	checkpointBuf := make([]byte, 8)
	if _, err := checkpointFile.Read(checkpointBuf); err != nil {
		return 0, err
	}
	checkpoint := binary.LittleEndian.Uint64(checkpointBuf)
	return checkpoint, nil
}

func (w *WAL) saveCheckpoint(checkpointFile *os.File, nextCheckpoint uint64) error {
	nextCheckpointBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(nextCheckpointBuf, nextCheckpoint)
	if err := checkpointFile.Truncate(0); err != nil {
		return err
	}
	if _, err := checkpointFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := checkpointFile.Write(nextCheckpointBuf); err != nil {
		return err
	}
	if err := checkpointFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Seek(delta int) error {
	if delta == 0 {
		return errors.New("Delta must be a non-zero integer")
	}
	currentReadIndexFile, err := w.OpenReadIndexHead()
	if err != nil {
		return err
	}
	defer currentReadIndexFile.Close()

	checkpointFile, err := w.openReadCheckpoint()
	if err != nil {
		return err
	}
	defer checkpointFile.Close()

	checkpoint, err := w.getCheckpoint(checkpointFile)
	if err != nil {
		return err
	}

	target := int(checkpoint) + delta

	currentReadIndexFileStat, err := currentReadIndexFile.Stat()
	if err != nil {
		return err
	}
	currentReadIndexFileSize := currentReadIndexFileStat.Size()
	maxRecords := int(currentReadIndexFileSize / 4)

	if target >= 0 && target < maxRecords {
		return w.saveCheckpoint(checkpointFile, uint64(target))
	}
	readIndexRef, err := w.openReadIndexRef()
	if err != nil {
		return err
	}
	defer readIndexRef.Close()
	if target >= maxRecords {
		nextIdx, err := w.hasNextIndex()
		if err != nil {
			return w.saveCheckpoint(checkpointFile, uint64(maxRecords))
		}
		if err = w.saveReadIndexHead(fmt.Sprintf("%09d", nextIdx)); err != nil {
			return err
		}
		if err = w.saveCheckpoint(checkpointFile, 0); err != nil {
			return err
		}
		newDelta := target - maxRecords
		return w.Seek(newDelta)

	} else {
		prevIdx, err := w.hasPreviousIndex()
		if err != nil {
			return w.saveCheckpoint(checkpointFile, 0)
		}
		if err = w.saveReadIndexHead(fmt.Sprintf("%09d", prevIdx)); err != nil {
			return err
		}
		stat, err := os.Stat(filepath.Join(w.IndexPathPrefix, fmt.Sprintf("%09d", prevIdx)))
		if err != nil {
			return err
		}
		prevMax := int(stat.Size() / 4)
		if err = w.saveCheckpoint(checkpointFile, uint64(prevMax)); err != nil {
			return err
		}
		return w.Seek(target)
	}
}

func (w *WAL) ReadNext(size int, delta int) ([]string, error) {
	if delta != 0 {
		if err := w.Seek(delta); err != nil {
			return nil, err
		}
	}
	readIndex, err := w.OpenReadIndexHead()
	if err != nil {
		return nil, err
	}
	defer readIndex.Close()

	checkpointFile, err := w.openReadCheckpoint()
	if err != nil {
		return nil, err
	}

	defer checkpointFile.Close()

	checkpoint, err := w.getCheckpoint(checkpointFile)
	if err != nil {
		return nil, err
	}

	byteOffset := checkpoint * 4

	pageSize := size * 4

	indexBuf := make([]byte, pageSize)

	n, err := readIndex.ReadAt(indexBuf, int64(byteOffset))
	if err != nil && err != io.EOF {
		return nil, err
	}

	if n < pageSize {
		// Reached end of segment
	}

	offsets := make([]uint32, 0, size)
	for i := 0; i < n; i += 4 {
		offset := binary.LittleEndian.Uint32(indexBuf[i : i+4])
		offsets = append(offsets, offset)
	}

	readHead, err := w.OpenReadHead()
	if err != nil {
		return nil, err
	}
	records := []string{}

	for _, offset := range offsets {
		recordLengthBuff := make([]byte, 4)
		if _, err := readHead.ReadAt(recordLengthBuff, int64(offset)); err != nil {
			return nil, err
		}
		recordLength := binary.LittleEndian.Uint32(recordLengthBuff)
		recordBuff := make([]byte, recordLength)
		if _, err := readHead.ReadAt(recordBuff, int64(offset+4)); err != nil {
			return nil, err
		}
		records = append(records, string(recordBuff))
	}
	defer readHead.Close()

	nextCheckpoint := checkpoint + uint64(len(records))
	if err = w.saveCheckpoint(checkpointFile, nextCheckpoint); err != nil {
		return nil, err
	}
	return records, nil
}

func (w *WAL) SeedWAL(size int) *[]error {
	var errs []error
	log.Println("Seeding WAL...")
	for v := range min(DEFAULT_SEED_SIZE, size) {
		record := fmt.Sprintf("%03d: %s", v+1, faker.Sentence())
		err := w.Append(record)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		log.Println("Done Seeding WAL...")
		return nil
	}
	return &errs
}
