package main

import "fmt"
import "os"
import "log"
import "github.com/random-j-farmer/bobstore"

func main() {
	if len(os.Args) == 1 {
		log.Fatal(`Usage:
bobstore ls DB
bobstore show DB 00000:00000000
bobstore gzip SRCDB DSTDB
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
		ref, err := bobstore.ParseRef(os.Args[3])
		if err != nil {
			log.Fatalf("can not parse ref: %s", os.Args[3])
		}

		blob, err := db.Read(ref)
		if err != nil {
			log.Fatalf("can not read ref %s: %v", ref, err)
		}

		fmt.Printf("%s", blob)
	} else if cmd == "gzip" {
		dstDB, err := bobstore.OpenRW(os.Args[3])
		defer dstDB.Close()

		if err != nil {
			log.Fatalf("bobstore.OpenRW %v", err)
		}

		cursor := db.Cursor(bobstore.Ref{})
		gzCodec := bobstore.GZIPCodec()
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

	} else {
		log.Fatalf("unknown command %s", cmd)
	}
}
