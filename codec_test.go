package bobstore

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/pkg/errors"
)

// forked gzip/gunzip to compare notes
func cmdline(program string, src []byte) ([]byte, error) {
	cmd := exec.Command(program)
	cmd.Stdin = bytes.NewBuffer(src)
	out := &bytes.Buffer{}
	cmd.Stdout = out
	err := cmd.Run()
	if err != nil {
		return nil, errors.Wrapf(err, "command %s failed", program)
	}
	return out.Bytes(), nil
}

func Test_DecodeGZIP(t *testing.T) {
	str := "I like Cake."
	gzip, err := cmdline("gzip", []byte(str))
	if err != nil {
		t.Errorf("gzip failed: %v", err)
	}
	gunzip, err := decodeGZIP(gzip)
	if err != nil {
		t.Errorf("DecodeGZIP failed: %v", err)
	}
	if str != string(gunzip) {
		t.Errorf("str<>gunzip(gzip(str)):\n%s\n%s", str, gunzip)
	}
}

func Test_EncodeGZIP(t *testing.T) {
	str := "EXTRA BIG STRING.  make it smaller!"
	gzip, err := encodeGZIP([]byte(str))
	if err != nil {
		t.Errorf("EncodeGZIP failed: %v", err)
	}
	gunzip, err := cmdline("gunzip", gzip)
	if err != nil {
		t.Errorf("gunzip failed: %v", err)
	}
	if str != string(gunzip) {
		t.Errorf("str<>gunzip(gzip(str)):\n%s\n%s", str, gunzip)
	}
}

func Test_Snappy(t *testing.T) {
	str := "i want some coffee, and make it snappy"
	snap, err := encodeSnappy([]byte(str))
	if err != nil {
		t.Errorf("encodesnappy: %v", err)
	}
	b, err := decodeSnappy(snap)
	if err != nil {
		t.Errorf("decode.snappy: %v", err)
	}
	if str != string(b) {
		t.Errorf("str<>b:%s\n%s", str, b)
	}
}
