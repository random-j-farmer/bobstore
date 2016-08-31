package bobstore

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

// writePosFile is the name of the write pos file
const writePosFile = "_writer"

// DB is an opaque handle to an opened blob storage
type DB struct {
	MaxFileLength uint32
	name          string
	writer        *os.File
	openflags     int
	lock          sync.Mutex
	writePos      Ref
	files         map[uint16]*dbFile
}

type dbFile struct {
	file *os.File
}

// Open a DB for reading
func Open(name string) (*DB, error) {
	db := &DB{
		name:          name,
		openflags:     os.O_RDONLY,
		files:         make(map[uint16]*dbFile),
		MaxFileLength: MaxFileLength,
	}
	return db, nil
}

// OpenRW opens a DB for RW access
func OpenRW(name string) (*DB, error) {
	db := &DB{
		name:          name,
		openflags:     os.O_RDWR | os.O_CREATE,
		files:         make(map[uint16]*dbFile),
		MaxFileLength: MaxFileLength,
	}

	err := os.MkdirAll(name, 0777)
	if err != nil {
		return nil, errors.Wrap(err, "mkdir failed")
	}

	wn := filepath.Join(name, writePosFile)
	db.writer, err = os.OpenFile(wn, db.openflags, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "openfile failed")
	}

	err = lockFile(wn, db.writer)
	if err != nil {
		db.Close()
		return nil, errors.Wrap(err, "LockFile failed")
	}

	err = readWriterRef(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// Close an open DB
func (db *DB) Close() (xerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// we are trying to close as much as we can,
	// but return at least one error if any happen
	if db.writer != nil {
		wn := filepath.Join(db.name, writePosFile)
		err := unlockFile(wn, db.writer)
		if err != nil {
			xerr = err
		}

		err = db.writer.Close()
		if err != nil {
			xerr = err
		}
	}

	for _, dbf := range db.files {
		err := dbf.file.Close()
		if err != nil {
			xerr = err
		}
	}

	db.files = nil

	// named return
	return
}
