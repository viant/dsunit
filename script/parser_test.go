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
/*!50003 SET SESSION SQL_MODE="" */;;
BEGIN
SELECT 1;
END;;

DELIMITER;
SELECT 2;`,
			SQLs: []string{
				`SELECT 1`,
				`/*!50003 SET SESSION SQL_MODE="" */`,
				`BEGIN
SELECT 1;
END`,
				`SELECT 2`,
			},
		},

		{
			description: "mysql delimiter",
			SQL: `SELECT 1;
DELIMITER;;
BEGIN
SELECT 1;
END;;
DELIMITER;
SELECT 2;
INSERT INTO abc(id_type, name) VALUES(3, 'Javascript ad; must be available XHTML (i.e. include script tags)');
SELECT 3;
`,
			SQLs: []string{
				`SELECT 1`,
				`BEGIN
SELECT 1;
END`,
				`SELECT 2`,
				`INSERT INTO abc(id_type, name) VALUES(3, 'Javascript ad; must be available XHTML (i.e. include script tags)')`,
				`SELECT 3`,
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
END;`,
				`INSERT INTO DUMMY(ID, NAME) VALUES(1, 'abc')`,
				`CREATE OR REPLACE TRIGGER users_before_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
SELECT users_seq.NEXTVAL
INTO   :new.id
FROM   dual;
END;`,
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




CREATE OR REPLACE FUNCTION insert_language_trigger() 
  RETURNS TRIGGER
  LANGUAGE plpgsql AS
  $$
  BEGIN
    NEW.code := '11';
    RETURN NEW;
  END
  $$;


INSERT INTO DUMMY(ID, NAME) VALUES(2, 'xyz');

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
  EXECUTE PROCEDURE insert_language_trigger()`, `CREATE OR REPLACE FUNCTION insert_language_trigger() 
  RETURNS TRIGGER
  LANGUAGE plpgsql AS
  $$
  BEGIN
    NEW.code := '11';
    RETURN NEW;
  END
  $$`,
				`INSERT INTO DUMMY(ID, NAME) VALUES(2, 'xyz')`,
			},
		},
		{
			description: "Create function, begin end blocks",
			SQL: `DELIMITER $$
START TRANSACTION $$

CREATE FUNCTION get_version()
	RETURNS INT
	READS SQL DATA
BEGIN
	DECLARE result INT DEFAULT 1;
	SELECT coalesce(max(version_number), 1) INTO result FROM version;
	RETURN result;
END $$

DROP PROCEDURE IF EXISTS set_version $$

CREATE PROCEDURE set_version(version INT)
BEGIN
	DELETE FROM version;
	INSERT INTO version VALUES(version);
END $$

COMMIT $$
DELIMITER ;
`,
			SQLs: []string{
				"START TRANSACTION",
				"CREATE FUNCTION get_version()\n\tRETURNS INT\n\tREADS SQL DATA\nBEGIN\n\tDECLARE result INT DEFAULT 1;\n\tSELECT coalesce(max(version_number), 1) INTO result FROM version;\n\tRETURN result;\nEND",
				"DROP PROCEDURE IF EXISTS set_version",
				"CREATE PROCEDURE set_version(version INT)\nBEGIN\n\tDELETE FROM version;\n\tINSERT INTO version VALUES(version);\nEND",
				"COMMIT",
			},
		},
		{
			description: "Comments and IF condition inside BEGIN-END block",
			SQL: `DELIMITER $$
DROP PROCEDURE IF EXISTS test_func $$
CREATE PROCEDURE test_func()
BEGIN
  IF get_version() = 17
  THEN
    -- Set this to false by default
    alter table TABLE_NAME add column IS_BLACK BOOLEAN DEFAULT FALSE;
    CALL set_version(18);
END IF;
END $$
DELIMITER ;
`,
			SQLs: []string{
				`DROP PROCEDURE IF EXISTS test_func`,
				`CREATE PROCEDURE test_func()
BEGIN
  IF get_version() = 17
  THEN
        alter table TABLE_NAME add column IS_BLACK BOOLEAN DEFAULT FALSE;
    CALL set_version(18);
END IF;
END`,
			},
		},
		{
			description: "'END;' string within BEGIN block before intended END;",
			SQL: `DELIMITER $$
DROP PROCEDURE IF EXISTS test_proc $$
CREATE PROCEDURE test_proc()
BEGIN
  IF get_version() = 14
  THEN
    DROP VIEW IF EXISTS TABLE_NAME;
    DROP VIEW IF EXISTS MONEY_SPEND;
    DROP VIEW IF EXISTS OTHER_TABLE;
    CALL set_version(15);
  END IF;
END $$
DELIMITER ;`,
			SQLs: []string{
				"DROP PROCEDURE IF EXISTS test_proc",
				`CREATE PROCEDURE test_proc()
BEGIN
  IF get_version() = 14
  THEN
    DROP VIEW IF EXISTS TABLE_NAME;
    DROP VIEW IF EXISTS MONEY_SPEND;
    DROP VIEW IF EXISTS OTHER_TABLE;
    CALL set_version(15);
  END IF;
END`,
			},
		},
	}

	for _, useCase := range useCases {
		actual := Parse(useCase.SQL)
		assert.EqualValues(t, useCase.SQLs, actual, useCase.description)
	}

}
