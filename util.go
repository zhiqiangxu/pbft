package pbft

import (
	"crypto/sha256"
	"encoding/hex"
)

// Quorum for pbft
func Quorum(n int) int {
	third := n / 3
	if third*3 < n {
		return n - third
	}

	return n - third + 1
}

// Hash for pbft
var Hash = sha256.Sum256

// Hash2String convert hash to string
var Hash2String = hex.EncodeToString

// Digest (x) = Hash2String(Hash(x))
func Digest(bytes []byte) string {
	hash := Hash(bytes)
	return Hash2String(hash[:])
}
