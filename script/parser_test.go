package script_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit/script"
	"strings"
	"testing"
)

//parseSQLScript parses sql script and breaks it down to submittable sql statements
func TestParseSQLScript(t *testing.T) {

	{
		sqlScript := `
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE users';
EXCEPTION
WHEN OTHERS THEN
IF SQLCODE != -942 THEN
RAISE;
END IF;
END;

CREATE TABLE users (
  id               NUMBER(5) PRIMARY KEY,
  username         VARCHAR2(255) DEFAULT NULL,
  active           NUMBER(1)    DEFAULT NULL,
  salary           DECIMAL(7, 2)  DEFAULT NULL,
  comments         VARCHAR2(255),
  modified         timestamp(0)
);

CREATE SEQUENCE users_seq START WITH 1;


CREATE OR REPLACE TRIGGER users_before_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
SELECT users_seq.NEXTVAL
INTO   :new.id
FROM   dual;
END;

`
		sqls := script.ParseSQLScript(strings.NewReader(sqlScript))
		assert.Equal(t, 4, len(sqls))
	}

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
