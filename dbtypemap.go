package dsunit

var dbTypeMappings = map[string]map[string]string{
	"bigquery": {

		"BOOLEAN": "BOOLEAN",
		"TINYINT": "BOOLEAN",

		"INT":      "INT64",
		"INT64":    "INT64",
		"INTEGER":  "INT64",
		"SMALLINT": "INT64",
		"BIGINT":   "INT64",

		"DECIMAL": "FLOAT64",
		"NUMERIC": "FLOAT64",
		"FLOAT":   "FLOAT64",
		"FLOAT64": "FLOAT64",
		"NUMBER":  "NUMERIC",

		"CHAR":     "STRING",
		"STRING":   "STRING",
		"VARCHAR":  "STRING",
		"VARCHAR2": "STRING",
		"CLOB":     "STRING",
		"TEXT":     "STRING",

		"DATE":        "TIMESTAMP",
		"DATETIME":    "TIMESTAMP",
		"TIMESTAMP":   "TIMESTAMP",
		"TIMESTAMPTZ": "TIMESTAMP",
	},

	"mysql": {
		"BOOLEAN": "TINYINT",
		"TINYINT": "TINYINT",

		"INT":      "INT",
		"INT64":    "BIGINT",
		"INTEGER":  "BIGINT",
		"SMALLINT": "SMALLINT",
		"BIGINT":   "BIGINT",

		"DECIMAL": "DECIMAL(7,2)",
		"NUMERIC": "DECIMAL(7,2)",
		"FLOAT64": "DECIMAL(7,2)",
		"FLOAT":   "DECIMAL(7,2)",
		"NUMBER":  "DECIMAL(7,2)",

		"CHAR":    "VARCHAR(255)",
		"VARCHAR": "VARCHAR(255)",
		"STRING":  "VARCHAR(255)",
		"CLOB":    "TEXT",
		"TEXT":    "TEXT",

		"DATE":        "DATE",
		"DATETIME":    "TIMESTAMP",
		"TIMESTAMP":   "TIMESTAMP",
		"TIMESTAMPTZ": "TIMESTAMP",
	},

	"pq": {

		"BOOLEAN": "BOOLEAN",
		"TINYINT": "BOOLEAN",

		"INT":      "INTEGER",
		"INT64":    "BIGINT",
		"INTEGER":  "INTEGER",
		"SMALLINT": "SMALLINT",
		"BIGINT":   "BIGINT",

		"DECIMAL": "NUMERIC(7,2)",
		"NUMERIC": "NUMERIC(7,2)",
		"FLOAT":   "NUMERIC(7,2)",
		"FLOAT64": "NUMERIC(7,2)",

		"VARCHAR": "VARCHAR(255)",
		"STRING":  "VARCHAR(255)",
		"CHAR":    "VARCHAR(255)",
		"CLOB":    "TEXT",
		"TEXT":    "TEXT",

		"TIMESTAMPTZ": "TIMESTAMPTZ",
		"DATE":        "TIMESTAMPTZ",
		"TIMESTAMP":   "TIMESTAMPTZ",
		"DATETIME":    "TIMESTAMPTZ",
	},

	"oracle": {
		"TINYINT": "NUMBER(1)",
		"BOOLEAN": "NUMBER(1)",

		"INT":      "NUMBER(7,0)",
		"INTEGER":  "NUMBER(7,0)",
		"INT64":    "NUMBER(14,0)",
		"SMALLINT": "NUMBER(5,0)",
		"BIGINT":   "NUMBER(14,0)",

		"FLOAT64": "NUMBER(7,2)",
		"NUMERIC": "NUMBER(7,2)",
		"FLOAT":   "NUMBER(7,2)",

		"CHAR":    "VARCHAR2(255)",
		"VARCHAR": "VARCHAR2(255)",
		"STRING":  "VARCHAR2(255)",
		"TEXT":    "VARCHAR2(255)",
		"CLOB":    "CLOB",

		"DATE":        "DATE",
		"TIMESTAMPTZ": "TIMESTAMP",
		"TIMESTAMP":   "TIMESTAMP",
		"DATETIME":    "TIMESTAMP",
	},
}
