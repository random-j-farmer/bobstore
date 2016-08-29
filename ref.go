package bobstore

import (
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/pkg/errors"
)

// Ref is a reference to a storage file and position
// the string representation is fixed: 5 digits : 8 hex digits ==> 14 characters in all
type Ref struct {
	// Fno
	Fno uint16
	// reserved
	_ uint16
	// Position within the file
	Pos uint32
}

const srefLength = 14

func (ref Ref) String() string {
	return fmt.Sprintf("%05d:%08x", ref.Fno, ref.Pos)
}

var parseRefRe = regexp.MustCompile(`^\d{5}:[0-9a-fA-F]{8}$`)

// ParseRef parses a refs string representation
func ParseRef(s string) (Ref, error) {
	var ref Ref

	if !parseRefRe.MatchString(s) {
		return ref, fmt.Errorf("can not parse Ref: %s", s)
	}

	fno, err := strconv.ParseUint(s[0:5], 10, 16)
	if err != nil {
		return ref, errors.Wrapf(err, "parse ref fno %s", s)
	}

	pos, err := strconv.ParseUint(s[6:], 16, 32)
	if err != nil {
		return ref, errors.Wrapf(err, "parse ref pos %s", s)
	}

	ref.Fno = uint16(fno)
	ref.Pos = uint32(pos)

	return ref, nil
}

func readWriterRef(db *DB) error {
	buff := make([]byte, srefLength)
	n, err := db.writer.ReadAt(buff, 0)
	if err == io.EOF && n == 0 {
		// if n == 0 and EOF --> empty file
		// start position here is 00000:00000000 which is the null value
		// log.Printf("readWriterRef: EOF n=%d", n)
		return nil
	}
	if err != nil {
		return err
	}
	if n < srefLength {
		return fmt.Errorf("incomplete ref %d", n)
	}

	ref, err := ParseRef(string(buff))
	if err != nil {
		return err
	}

	// log.Printf("readWriterRef: parsed %s into %s", buff, ref.String())
	db.writePos = ref

	return nil
}

func writeWriterRef(db *DB) error {
	sref := db.writePos.String()
	_, err := db.writer.WriteAt([]byte(sref), 0)
	if err != nil {
		return err
	}

	err = db.writer.Truncate(srefLength)
	if err != nil {
		return err
	}

	return nil
}
