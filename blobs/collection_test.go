package blobs

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/tmc/go-iroh/endpointticket"
)

func TestCollectionMetadataRustVector(t *testing.T) {
	c := NewCollection([]CollectionEntry{
		{Name: "test", Hash: NewHash([]byte("unused1"))},
		{Name: "a", Hash: NewHash([]byte("unused2"))},
		{Name: "b", Hash: NewHash([]byte("unused3"))},
	})
	const want = "436f6c6c656374696f6e56302e03047465737401610162"
	if got := hex.EncodeToString(c.MetadataBytes()); got != want {
		t.Fatalf("MetadataBytes = %s, want %s", got, want)
	}
	if got := NewHash(c.MetadataBytes()).String(); got != "2c517068ff5bd81ae00fbac1654887127548f87892b9715faf3d1535b8f4b3ff" {
		t.Fatalf("metadata hash = %s", got)
	}
}

func TestCollectionRoundTrip(t *testing.T) {
	child1 := NewHash([]byte("first"))
	child2 := NewHash([]byte("second"))
	c := NewCollection([]CollectionEntry{
		{Name: "one.txt", Hash: child1},
		{Name: "two.txt", Hash: child2},
	})
	const (
		wantMeta = "436f6c6c656374696f6e56302e02076f6e652e7478740774776f2e747874"
		wantRoot = "e5477f9b22b6612d99436e6fea723b79d0034cc95ab804c20372d90a722f69e9"
	)
	if got := hex.EncodeToString(c.MetadataBytes()); got != wantMeta {
		t.Fatalf("MetadataBytes = %s, want %s", got, wantMeta)
	}
	if got := c.Root().String(); got != wantRoot {
		t.Fatalf("Root = %s, want %s", got, wantRoot)
	}
	got, err := ParseCollection(c.HashSequence(), c.MetadataBytes())
	if err != nil {
		t.Fatalf("ParseCollection: %v", err)
	}
	if entries := got.Entries(); len(entries) != 2 || entries[0].Name != "one.txt" || entries[0].Hash != child1 || entries[1].Name != "two.txt" || entries[1].Hash != child2 {
		t.Fatalf("Entries = %+v", entries)
	}
}

func TestGetCollectionBytes(t *testing.T) {
	payloads := [][]byte{
		[]byte("first child"),
		vectorData(BlockSize + 1),
	}
	var entries []CollectionEntry
	blobs := make(map[Hash][]byte)
	for i, data := range payloads {
		hash := NewHash(data)
		entries = append(entries, CollectionEntry{Name: string(rune('a'+i)) + ".bin", Hash: hash})
		blobs[hash] = append([]byte(nil), data...)
	}
	collection := NewCollection(entries)
	meta := collection.MetadataBytes()
	seq := collection.HashSequence()
	blobs[NewHash(meta)] = meta
	blobs[collection.Root()] = seq.Bytes()

	client, server := newTestBidiStreamPair()
	errc := make(chan error, 1)
	go func() {
		errc <- ServeBlob(context.Background(), server, mustStore(t, blobValues(blobs)...))
	}()
	gotCollection, got, err := GetCollectionBytes(context.Background(), client, collection.Root())
	if err != nil {
		t.Fatalf("GetCollectionBytes: %v", err)
	}
	if entries := gotCollection.Entries(); len(entries) != len(payloads) {
		t.Fatalf("collection entries = %d, want %d", len(entries), len(payloads))
	}
	for i := range payloads {
		if !bytes.Equal(got[i], payloads[i]) {
			t.Fatalf("blob %d length = %d, want %d", i, len(got[i]), len(payloads[i]))
		}
	}
	if err := <-errc; err != nil {
		t.Fatalf("ServeBlob: %v", err)
	}
}

func TestParseCollectionErrors(t *testing.T) {
	_, err := ParseCollection(HashSequence{}, nil)
	if err == nil {
		t.Fatal("ParseCollection accepted empty hash sequence")
	}

	c := NewCollection([]CollectionEntry{{Name: "x", Hash: NewHash([]byte("x"))}})
	meta := append([]byte(nil), c.MetadataBytes()...)
	meta[len(meta)-1] = 'y'
	if _, err := ParseCollection(c.HashSequence(), meta); err == nil {
		t.Fatal("ParseCollection accepted wrong metadata hash")
	}
	if _, err := ParseCollection(c.HashSequence(), []byte("bad")); err == nil {
		t.Fatal("ParseCollection accepted bad metadata")
	}

	bad := append(c.MetadataBytes(), 0)
	_, err = ParseCollection(NewHashSequence([]Hash{NewHash(bad), NewHash([]byte("x"))}), bad)
	if !errors.Is(err, endpointticket.ErrTrailingBytes) {
		t.Fatalf("ParseCollection trailing error = %v", err)
	}

	hugeCount := append([]byte(collectionHeader), 0xff, 0xff, 0xff, 0xff, 0x0f)
	_, err = ParseCollection(NewHashSequence([]Hash{NewHash(hugeCount)}), hugeCount)
	if !errors.Is(err, endpointticket.ErrTruncated) {
		t.Fatalf("ParseCollection huge count error = %v", err)
	}

	invalidUTF8 := append([]byte(collectionHeader), 1, 1, 0xff)
	_, err = ParseCollection(NewHashSequence([]Hash{NewHash(invalidUTF8), NewHash([]byte("x"))}), invalidUTF8)
	if err == nil {
		t.Fatal("ParseCollection accepted invalid UTF-8")
	}
}
