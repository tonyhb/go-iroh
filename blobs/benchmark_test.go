package blobs

import (
	"context"
	"io"
	"testing"
)

var benchmarkBlobSink Hash

func benchmarkBlobData(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*31 + i/7)
	}
	return b
}

func BenchmarkHash(b *testing.B) {
	data := benchmarkBlobData(1 << 20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkBlobSink = NewHash(data)
	}
}

func BenchmarkBAOEncodeDecode(b *testing.B) {
	data := benchmarkBlobData(1 << 20)
	hash, encoded, err := EncodeBlob(data)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			h, _, err := EncodeBlob(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBlobSink = h
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := DecodeBlob(hash, encoded)
			if err != nil {
				b.Fatal(err)
			}
			if len(out) != len(data) {
				b.Fatalf("decoded %d bytes, want %d", len(out), len(data))
			}
		}
	})
}

func BenchmarkFSStorePutGet(b *testing.B) {
	ctx := context.Background()
	data := benchmarkBlobData(64 << 10)
	store, err := NewFSStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	hash, err := store.Add(data)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("put", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			h, err := store.Add(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBlobSink = h
		}
	})
	b.Run("get", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			entry, err := store.Open(ctx, hash)
			if err != nil {
				b.Fatal(err)
			}
			r, err := entry.DataReader(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, io.NewSectionReader(r, 0, int64(len(data)))); err != nil {
				b.Fatal(err)
			}
		}
	})
}
