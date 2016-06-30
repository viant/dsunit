package dsunit

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"strings"
)


//parseSQLScript parses sql script and breaks it down to submittable sql statements
func TestParseSQLScript(t *testing.T) {

	{
		sqlScript := "SELECT 1;\nSELECT 2;"
		sqls := parseSQLScript(strings.NewReader(sqlScript))
		assert.Equal(t, 2, len(sqls))
	}


	{
		sqlScript := "SELECT 1;\nDELIMITER;;\nBEGIN\nSELECT 1;\nEND;;\nDELIMITER;\nSELECT 2;"
		sqls := parseSQLScript(strings.NewReader(sqlScript))
		assert.Equal(t, 3, len(sqls))
		assert.Equal(t, "\nBEGIN\nSELECT 1;\nEND", sqls[1])
	}

}
