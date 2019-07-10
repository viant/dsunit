package dsunit

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegisterRequest_Validate(t *testing.T) {

	records := []interface{}{
		map[string]interface{}{
			"@fromQuery@": "SELECT *  FROM users where id \u003c= 2 ORDER BY id",
			"@indexBy@":   []string{"id"},
		},
		map[string]interface{}{
			"id": 1,
		},
	}
	actual := removeDirectiveRecord(records)
	assert.EqualValues(t, []interface{}{
		map[string]interface{}{
			"id": 1,
		},
	}, actual, "should remove directive row")

}
