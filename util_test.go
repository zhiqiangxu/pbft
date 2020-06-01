package pbft

import (
	"testing"

	"gotest.tools/assert"
)

func TestQuorum(t *testing.T) {
	q := Quorum(5)
	assert.Assert(t, q == 4)

	q = Quorum(7)
	assert.Assert(t, q == 5)
}
