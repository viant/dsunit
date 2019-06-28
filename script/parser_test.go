package script

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParse(t *testing.T) {

	var useCases = []struct {
		description string
		SQL         string
		SQLs        []string
	}{

		{
			description: "mysql delimiter",
			SQL: `SELECT 1;
DELIMITER;;
BEGIN
SELECT 1;
END;;
DELIMITER;
SELECT 2;`,
			SQLs: []string{
				`SELECT 1`,
				`BEGIN
SELECT 1;
END`,
				`SELECT 2`,
			},
		},
		{
			description: "PLSQL plSQLBlock",
			SQL: `BEGIN
EXECUTE IMMEDIATE 'DROP TABLE users';
EXCEPTION
WHEN OTHERS THEN
IF SQLCODE != -942 THEN
RAISE;
END IF;
END;

INSERT INTO DUMMY(ID, NAME) VALUES(1, 'abc');

CREATE OR REPLACE TRIGGER users_before_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
SELECT users_seq.NEXTVAL
INTO   :new.id
FROM   dual;
END;

INSERT INTO DUMMY(ID, NAME) VALUES(2, 'xyz');

`,
			SQLs: []string{
				`BEGIN
EXECUTE IMMEDIATE 'DROP TABLE users';
EXCEPTION
WHEN OTHERS THEN
IF SQLCODE != -942 THEN
RAISE;
END IF;
END`,
				`INSERT INTO DUMMY(ID, NAME) VALUES(1, 'abc')`,
				`CREATE OR REPLACE TRIGGER users_before_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
SELECT users_seq.NEXTVAL
INTO   :new.id
FROM   dual;
END`,
				`INSERT INTO DUMMY(ID, NAME) VALUES(2, 'xyz')`,
			},
		},

		{
			description: "regular SQL",
			SQL: `
		CREATE TABLE users (
		 id               NUMBER(5) PRIMARY KEY,
		 username         VARCHAR2(255) DEFAULT NULL,
		 active           NUMBER(1)    DEFAULT NULL,
		 salary           DECIMAL(7, 2)  DEFAULT NULL,
		 comments         VARCHAR2(255),
		 modified         timestamp(0)
		);
		
		CREATE SEQUENCE users_seq START WITH 1;
		`,
			SQLs: []string{
				`CREATE TABLE users (
		 id               NUMBER(5) PRIMARY KEY,
		 username         VARCHAR2(255) DEFAULT NULL,
		 active           NUMBER(1)    DEFAULT NULL,
		 salary           DECIMAL(7, 2)  DEFAULT NULL,
		 comments         VARCHAR2(255),
		 modified         timestamp(0)
		)`,
				`CREATE SEQUENCE users_seq START WITH 1`,
			},
		},

		{
			description: "posgress delimitered",
			SQL: `CREATE TABLE words(
  id NUMERIC NOT NULL PRIMARY KEY,
  language CHAR(2) NOT NULL REFERENCES languages,
  name TEXT UNIQUE NOT NULL
);

CREATE FUNCTION insert_language_trigger() 
  RETURNS TRIGGER
  LANGUAGE plpgsql AS
  $$
  BEGIN
    NEW.code := '11';
    RETURN NEW;
  END
  $$;

CREATE TRIGGER insert_language BEFORE INSERT 
  ON languages
  FOR EACH ROW
  EXECUTE PROCEDURE insert_language_trigger();
`,
			SQLs: []string{
				`CREATE TABLE words(
  id NUMERIC NOT NULL PRIMARY KEY,
  language CHAR(2) NOT NULL REFERENCES languages,
  name TEXT UNIQUE NOT NULL
)`,
				`CREATE FUNCTION insert_language_trigger() 
  RETURNS TRIGGER
  LANGUAGE plpgsql AS
  $$
  BEGIN
    NEW.code := '11';
    RETURN NEW;
  END
  $$`,
				`CREATE TRIGGER insert_language BEFORE INSERT 
  ON languages
  FOR EACH ROW
  EXECUTE PROCEDURE insert_language_trigger()`,
			},
		},
	}

	for _, useCase := range useCases {
		actual := Parse(useCase.SQL)
		assert.EqualValues(t, useCase.SQLs, actual, useCase.description)
	}

}
