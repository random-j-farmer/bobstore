package main

import "fmt"
import "os"
import "log"
import "github.com/random-j-farmer/bobstore"
import "encoding/json"
import "crypto/sha1"

func main() {
	if len(os.Args) == 1 {
		log.Fatal(`Usage:
bobstore ls DB
bobstore show DB 00000:00000000
bobstore gzip SRCDB DSTDB
bobstore snap SRCDB DSTDB
bobstore json SRCDB
`)
	}

	dbName := os.Args[2]
	db, err := bobstore.Open(dbName)
	if err != nil {
		log.Fatalf("can not open bobs db: %v", err)
	}

	cmd := os.Args[1]
	if cmd == "ls" {
		cursor := db.Cursor(bobstore.Ref{})
		for cursor.Next() {
			ratio := float64(cursor.Compressed()) / float64(cursor.Length())
			fmt.Printf("%s %s %d/%d %g\n", cursor.Ref(), cursor.Typ(), cursor.Compressed(), cursor.Length(), ratio)
		}
		if cursor.Error() != nil {
			log.Fatalf("cursor.next: %v", cursor.Error())
		}
	} else if cmd == "show" {
		var ref bobstore.Ref
		ref, err = bobstore.ParseRef(os.Args[3])
		if err != nil {
			log.Fatalf("can not parse ref: %s", os.Args[3])
		}

		var blob []byte
		blob, err = db.Read(ref)
		if err != nil {
			log.Fatalf("can not read ref %s: %v", ref, err)
		}

		fmt.Printf("%s", blob)
	} else if cmd == "gzip" {
		err = copyDB(db, os.Args[3], "GZIP")
		if err != nil {
			log.Fatalf("copy error: %v", err)
		}
	} else if cmd == "snap" {
		err = copyDB(db, os.Args[3], "SNAP")
		if err != nil {
			log.Fatalf("copy error: %v", err)
		}
	} else if cmd == "json" {
		err = exportJSON(db)
		if err != nil {
			log.Fatalf("json error: %v", err)
		}
	} else {
		log.Fatalf("unknown command %s", cmd)
	}
}

func copyDB(db *bobstore.DB, dst, codec string) error {
	dstDB, err := bobstore.OpenRW(dst)
	defer dstDB.Close()

	if err != nil {
		log.Fatalf("bobstore.OpenRW %s: %v", dst, err)
	}

	cursor := db.Cursor(bobstore.Ref{})
	gzCodec := bobstore.CodecFor(codec)
	for cursor.Next() {
		fmt.Printf("%s\n", cursor.Ref())
		b, err := db.Read(cursor.Ref())
		if err != nil {
			log.Printf("error reading %s: %v", cursor.Ref(), err)
		}

		ref2, err := dstDB.WriteWithCodec(b, gzCodec)
		if err != nil {
			log.Printf("error writing %s: %v", cursor.Ref(), err)
		}
		fmt.Printf("new ref: %s\n", ref2)
	}
	if cursor.Error() != nil {
		log.Fatalf("cursor.next: %v", cursor.Error())
	}

	return nil
}

func exportJSON(db *bobstore.DB) error {
	cursor := db.Cursor(bobstore.Ref{})
	for cursor.Next() {
		b, err := db.Read(cursor.Ref())
		if err != nil {
			log.Fatalf("db.Read: %v", err)
		}
		var js interface{}
		err = json.Unmarshal(b, &js)
		if err != nil {
			log.Fatalf("json.Unmarshal: %v", err)
		}

		m := make(map[string]interface{})
		m["stored"] = js
		m["ref"] = cursor.Ref().String()
		m["sha1"] = fmt.Sprintf("%0x", sha1.Sum(b))

		marsh, err := json.Marshal(m)
		if err != nil {
			log.Fatalf("json.Marshal: %v", err)
		}
		fmt.Printf("%s\n", marsh)
	}
	if cursor.Error() != nil {
		log.Fatalf("cursor.next: %v", cursor.Error())
	}

	return nil
}
