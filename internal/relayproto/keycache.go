package relayproto

import (
	"sync"

	"github.com/tmc/go-iroh/key"
)

const keyCacheCapacity = 4096

// keyCache memoises curve-point validation of endpoint IDs: every relayed
// datagram carries one, and validating costs a field square root. Mirrors
// Rust iroh-relay's KeyCache.
type keyCache struct {
	mu sync.RWMutex
	m  map[[key.PublicKeySize]byte]struct{}
}

var validKeys = keyCache{m: make(map[[key.PublicKeySize]byte]struct{})}

func (c *keyCache) endpointID(b []byte) (key.EndpointID, error) {
	if len(b) != key.PublicKeySize {
		return key.EndpointID{}, key.ErrInvalidKeyLength
	}
	arr := [key.PublicKeySize]byte(b)
	c.mu.RLock()
	_, ok := c.m[arr]
	c.mu.RUnlock()
	if ok {
		return key.UncheckedEndpointID(arr), nil
	}
	id, err := key.NewEndpointID(arr)
	if err != nil {
		return key.EndpointID{}, err
	}
	c.mu.Lock()
	if len(c.m) >= keyCacheCapacity {
		for k := range c.m { // random eviction
			delete(c.m, k)
			break
		}
	}
	c.m[arr] = struct{}{}
	c.mu.Unlock()
	return id, nil
}
