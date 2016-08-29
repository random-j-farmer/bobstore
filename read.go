package bobstore

import (
	"io"
	"unsafe"

	"github.com/pkg/errors"
)

// Read the blob at ref
func (db *DB) Read(ref Ref) ([]byte, error) {
	f, err := getFile(db, ref.Fno)
	if err != nil {
		return nil, err
	}

	var hb [headerSize]byte
	_, err = f.ReadAt(hb[:], int64(ref.Pos))
	if err != nil {
		return nil, errors.Wrapf(err, "read failed for %s", ref)
	}
	h := (*header)(unsafe.Pointer(&hb[0]))

	compressed := make([]byte, h.Compressed)
	_, err = f.ReadAt(compressed, int64(ref.Pos+headerSize))
	if err != nil {
		return nil, errors.Wrapf(err, "read failed for %s", ref)
	}

	codec := codecs[string(h.Typ[:])]
	if codec == nil {
		return nil, errors.Wrapf(err, "unknown codec %s for %s", h.Typ, ref)
	}

	b, err := codec.decoder(compressed)
	if err != nil {
		return nil, errors.Wrapf(err, "%s.decode %s", h.Typ, ref)
	}

	return b, nil
}

// Cursor keeps track of the current position and record for iteration.
type Cursor struct {
	db         *DB
	next       Ref
	ref        Ref
	typ        string
	length     uint32
	compressed uint32
	err        error
}

// Cursor iterates over the db.
// next is the initial ref, null value means beginning of DB.
func (db *DB) Cursor(next Ref) *Cursor {
	return &Cursor{db: db, next: next}
}

// Next advances to the next blob.
//
// It returns true if there is a current blob in which
// case the methods returning information on the current blob
// can be called.  It returns false after the last blob
// has been visited, or after an error occurred.
//
// Before Next() is called the first time, all other
// method results are undefined.  After Next() returned
// false, only Error() has a defined result.
//
func (c *Cursor) Next() bool {
	f, err := getFile(c.db, c.next.Fno)
	if err != nil {
		c.err = err
		return false
	}

	var hb [headerSize]byte
	_, err = f.ReadAt(hb[:], int64(c.next.Pos))
	// handle switch to next file
	if err == io.EOF {
		if hasFile(c.db, c.next.Fno+1) {
			c.next.Fno++
			c.next.Pos = 0
			return c.Next()
		}
		return false
	}
	if err != nil {
		c.err = err
		return false
	}
	h := (*header)(unsafe.Pointer(&hb[0]))

	c.ref = Ref{Fno: c.next.Fno, Pos: c.next.Pos}
	c.typ = string(h.Typ[:])
	c.length = h.Length
	c.compressed = h.Compressed

	c.next.Pos = (c.next.Pos + headerSize + h.Compressed + 7) & 0xFFFFFFF8

	return true
}

// Ref returns the current ref.
func (c *Cursor) Ref() Ref {
	return c.ref
}

// Typ returns the typ of the current blob.
// One of SNAP, GZIP, NONE.
func (c *Cursor) Typ() string {
	return c.typ
}

// Length returns the length of the current blob.
func (c *Cursor) Length() uint32 {
	return c.length
}

// Compressed returns the compressed length of the current blob.
func (c *Cursor) Compressed() uint32 {
	return c.compressed
}

// Err gives the error that caused Next() to return false, if any.
func (c *Cursor) Error() error {
	return c.err
}
