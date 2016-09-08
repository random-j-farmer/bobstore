package bobstore

import (
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/pkg/errors"
)

// MaxFileLength  hard max for length of single file: 1GB
// this means a single blob may not be larger than 1GB - headerSize
const MaxFileLength = 1024 * 1024 * 1024

// MaxNumberFiles is 64k
const MaxNumberFiles = 0xFFFF

// headerSize 16 bytes
const headerSize = 16

// the header precdes every blob
type header struct {
	// typ - one of BLOB (plain blob), SNAP (snap compressed), GZIP (gzip compressed)
	Typ [4]byte

	// reserved for alignment purposes
	_ uint32

	// uncompressed length
	Length uint32

	// compressed length
	// compressed bytes follow, followed by padding rouding up to 8
	// i.e. a header is always 64bit aligned
	Compressed uint32
}

type headerBytes [headerSize]byte

// Write to the database.  Will use the default SnappyCodec()
func (db *DB) Write(b []byte) (Ref, error) {
	return db.WriteWithCodec(b, snappyCodec)
}

// WriteWithCodec - write the blob with explicit codec.
func (db *DB) WriteWithCodec(b []byte, codec *Codec) (Ref, error) {
	var ref Ref

	dst, err := codec.encoder(b)
	if err != nil {
		return ref, errors.Wrapf(err, "encoding %s", codec.typ)
	}

	h := header{Length: uint32(len(b)), Compressed: uint32(len(dst))}
	copy(h.Typ[:], []byte(codec.typ))

	f, ref, err := reserve(db, &h)
	if err != nil {
		return ref, errors.Wrap(err, "reserve")
	}

	// XXX: errors here will leave a  blob with errors
	// maybe we should hold the mutex for the whole write, after all
	// possible solution: mark as type ERRO

	_, err = f.WriteAt((*headerBytes)(unsafe.Pointer(&h))[:], int64(ref.Pos))
	if err != nil {
		return ref, errors.Wrap(err, "compress failed")
	}

	sizeWithPadding := (len(dst) + 7) & 0xFFFFFFF8
	if cap(dst) < sizeWithPadding {
		var b8 [8]byte
		dst = append(dst, b8[:sizeWithPadding-len(dst)]...)
	}
	_, err = f.WriteAt(dst[:sizeWithPadding], int64(ref.Pos+headerSize))
	if err != nil {
		return ref, errors.Wrap(err, "compress failed")
	}

	return ref, nil
}

// write the header and return the positition and length for a write
// of the header and blob data.  the increasing of the write position has
// to be protected by a mutex.
func reserve(db *DB, h *header) (*os.File, Ref, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// header + compressed size rounded up to the next multiple of 8
	need := (headerSize + h.Compressed + 7) & 0xFFFFFFF8

	// next file if insufficient space
	if db.writePos.Pos+need > db.MaxFileLength {
		if db.writePos.Fno == MaxNumberFiles {
			panic(fmt.Errorf("maximum number of files already in use: %d", MaxNumberFiles))
		}
		db.writePos.Fno++
		db.writePos.Pos = 0
	}

	f, err := xGetFile(db, db.writePos.Fno)
	if err != nil {
		return nil, Ref{}, err
	}

	// return the values before increasing the write position
	pos := db.writePos.Pos

	// increase write position
	db.writePos.Pos += need

	// now write it
	_, err = db.writer.WriteAt([]byte(db.writePos.String()), 0)
	if err != nil {
		return nil, Ref{}, errors.Wrap(err, "write failed")
	}

	return f, Ref{Fno: db.writePos.Fno, Pos: pos}, nil
}

func getFile(db *DB, fno uint16) (*os.File, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return xGetFile(db, fno)
}

// x means mutex is acquired
func xGetFile(db *DB, fno uint16) (*os.File, error) {
	if f := db.files[fno]; f != nil {
		return f.file, nil
	}

	f, err := os.OpenFile(filepath.Join(db.name, fmt.Sprintf("%05d", fno)), db.openflags, 0666)
	if err != nil {
		return nil, err
	}

	db.files[fno] = &dbFile{f}

	// XXX: mark file as recently used, close files that have been open too long

	return f, nil
}
