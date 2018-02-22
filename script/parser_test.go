package script_test

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"github.com/viant/dsunit/script"
)

//parseSQLScript parses sql script and breaks it down to submittable sql statements
func TestParseSQLScript(t *testing.T) {

	{
		sqlScript := "SELECT 1;\nSELECT 2;"
		sqls := script.ParseSQLScript(strings.NewReader(sqlScript))
		assert.Equal(t, 2, len(sqls))
	}

	{
		sqlScript := "SELECT 1;\nDELIMITER;;\nBEGIN\nSELECT 1;\nEND;;\nDELIMITER;\nSELECT 2;"
		sqls := script.ParseSQLScript(strings.NewReader(sqlScript))
		assert.Equal(t, 3, len(sqls))
		assert.Equal(t, "\nBEGIN\nSELECT 1;\nEND", sqls[1])
	}

	{
		sqlScript := "SELECT 1;\nDELIMITER;;\nBEGIN\nSELECT 1;\nEND;;\nDELIMITER;\nSELECT 2;"
		sqls := script.ParseSQLScript(strings.NewReader(sqlScript))
		assert.Equal(t, 3, len(sqls))
		assert.Equal(t, "\nBEGIN\nSELECT 1;\nEND", sqls[1])
	}

}
