package bobstore

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var testDB *DB

func TestMain(m *testing.M) {
	var rc = 0
	func() {
		name, err := ioutil.TempDir("", "bobs")
		if err != nil {
			panic("can not open test directory")
		}

		testDB, err = OpenRW(name)
		if err != nil {
			log.Panicf("can not open test db: %s %v", name, err)
		}
		defer testDB.Close()

		rc = m.Run()
	}()

	os.Exit(rc)
}

func Test_OpenRW(t *testing.T) {
	db, err := OpenRW(testDB.name)
	if err == nil {
		t.Errorf("could open same db twice")
		db.Close()
	}
}

func Test_Open(t *testing.T) {
	db, err := Open(testDB.name)
	if err != nil {
		// NOTE: you should not open the same db twice in the same process
		// it's safe to use the same one from multiple go routines
		t.Errorf("could not open same db for reading")
	} else {
		db.Close()
	}
}

func Test_Write(t *testing.T) {
	blob := "i am a little blob and i am ok"
	ref, err := testDB.Write([]byte(blob))
	if err != nil {
		t.Errorf("compress failed: %v", err)
	}
	t.Logf("compress result: %#v", ref)
	if ref.Fno != 0 || ref.Pos != 0 {
		t.Errorf("ref should be 00000:00000000: %s", ref)
	}

	if testDB.writePos.Pos == 0 {
		t.Errorf("db writepos should not be zero any more: %s", testDB.writePos)
	}

	wposFile, _ := os.Open(filepath.Join(testDB.name, writePosFile))
	defer wposFile.Close()
	wpos, _ := ioutil.ReadAll(wposFile)
	if string(wpos) != testDB.writePos.String() {
		t.Errorf("write position on file should have been: %s but was %s", testDB.writePos, wpos)
	}

	littleBlob := "little bob"
	ref2, err := testDB.Write([]byte(littleBlob))
	if err != nil {
		t.Errorf("compress blob2 failed: %v", err)
	}
	t.Logf("compress blob2 result: %v", ref2)

	// now read it
	blob2, err := testDB.Read(ref)
	if err != nil {
		t.Errorf("error reading ref %s: %v", ref, err)
	}

	if blob != string(blob2) {
		t.Errorf("orig<>read back:\n%s\n%s", blob, blob2)
	}
}

func Test_Cursor(t *testing.T) {
	c := testDB.Cursor(Ref{})
	cnt := 0
	for c.Next() {
		t.Logf("Test_Cursor: %s - %d", c.Ref(), c.Length())
		if c.Typ() != "SNAP" {
			t.Errorf("typ should have been SNAP, but: %s", c.Typ())
		}
		cnt++
	}
	if cnt != 2 {
		t.Errorf("should have iterated twice, but: %d", cnt)
	}
}

func Test_Refs(t *testing.T) {
	ref := Ref{Fno: 3, Pos: 0x666}
	refStr := ref.String()
	if refStr != "00003:00000666" {
		t.Errorf("ref str expected<>actual\n%s\n%s", "00003:00000666", refStr)
	}

	ref2, err := ParseRef(refStr)
	if err != nil {
		t.Errorf("error parsing ref %s", refStr)
	}

	if ref2.Fno != ref.Fno || ref2.Pos != ref.Pos {
		t.Errorf("error parsing ref: exp<>act\n%s\n%s", refStr, ref2.String())
	}
}
