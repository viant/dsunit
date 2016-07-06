package dsunit_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
)

func TestWithinPredicate(t *testing.T) {
	targetTime := time.Unix(1465009041, 0)
	predicate := dsunit.NewWithinPredicate(targetTime, -2, "")
	timeValue := time.Unix(1465009042, 0)
	assert.True(t, predicate.Apply(timeValue))

	timeValue = time.Unix(1465009044, 0)
	assert.False(t, predicate.Apply(timeValue))
}
