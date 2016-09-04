package bobstore

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

// Codec for compression/decompression
type Codec struct {
	typ     string
	encoder func([]byte) ([]byte, error)
	decoder func([]byte) ([]byte, error)
}

func encodeGZIP(src []byte) ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, len(src)/5))
	buff.Reset()

	w := gzip.NewWriter(buff)
	_, err := w.Write(src)
	if err != nil {
		return nil, errors.Wrap(err, "gzip.write")
	}

	err = w.Close()
	if err != nil {
		return nil, errors.Wrap(err, "gzip.close")
	}

	return buff.Bytes(), nil
}

func decodeGZIP(src []byte) ([]byte, error) {
	in := bytes.NewBuffer(src)
	r, err := gzip.NewReader(in)
	if err != nil {
		return nil, errors.Wrap(err, "gzip.NewReader")
	}

	dst, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "gzip.read")
	}

	err = r.Close()
	if err != nil {
		return nil, errors.Wrap(err, "gzip.close")
	}

	return dst, nil
}

func encodeSnappy(src []byte) ([]byte, error) {
	dst := make([]byte, len(src)/4)
	dst = snappy.Encode(dst, src)
	return dst, nil
}

func decodeSnappy(src []byte) ([]byte, error) {
	dst, err := snappy.Decode(make([]byte, len(src)*8), src)
	if err != nil {
		return nil, errors.Wrapf(err, "snappy.Decode")
	}
	return dst, nil
}

var snappyCodec = &Codec{
	typ:     "SNAP",
	encoder: encodeSnappy,
	decoder: decodeSnappy,
}

var gzipCodec = &Codec{
	typ:     "GZIP",
	encoder: encodeGZIP,
	decoder: decodeGZIP,
}

var codecs = make(map[string]*Codec)

func init() {
	codecs["SNAP"] = snappyCodec
	codecs["GZIP"] = gzipCodec
}

// CodecFor returns the codec for name
func CodecFor(name string) *Codec {
	return codecs[name]
}

// SnappyCodec - snapy codec
func SnappyCodec() *Codec {
	return snappyCodec
}

// GZIPCodec - gzip codec
func GZIPCodec() *Codec {
	return gzipCodec
}
