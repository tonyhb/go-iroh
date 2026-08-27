package blobs

import (
	"bytes"
	"fmt"
	"testing"

	"lukechampine.com/blake3/bao"
)

// TestFullRangeEqualsEncodeBlob pins the premise under writeBlob: a BAO slice
// covering the whole blob is byte-identical to a full-blob encoding.
//
// writeBlob serves complete blobs by extracting the range [0, size), which is
// only wire-correct while that equality holds. If the framing bao.Encode
// produces ever diverges between the two paths, writeBlob would silently emit
// bytes that differ from EncodeBlob and no other test in the suite would
// notice. The sizes below straddle the 1024-byte block boundary in both
// directions, since that is where a framing change would first show up.
func TestFullRangeEqualsEncodeBlob(t *testing.T) {
	sizes := []int{0, 1, 1023, 1024, 1025, 65536, 200000}
	for _, size := range sizes {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i)
			}

			_, want, err := EncodeBlob(data)
			if err != nil {
				t.Fatalf("EncodeBlob: %v", err)
			}

			outboard, _ := bao.EncodeBuf(data, 4, true)
			var got bytes.Buffer
			if err := ExtractBlobRange(&got, bytes.NewReader(data), bytes.NewReader(outboard), 0, uint64(size)); err != nil {
				t.Fatalf("ExtractBlobRange: %v", err)
			}

			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("full-range extraction differs from EncodeBlob (%d vs %d bytes)", got.Len(), len(want))
			}
		})
	}
}
