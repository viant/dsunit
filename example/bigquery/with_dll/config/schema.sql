CREATE OR REPLACE TABLE users (
  id         INT64,
  username   STRING,
  active     BOOLEAN,
  modified   TIMESTAMP,
  salary FLOAT64,
  comments STRING
);



CREATE OR REPLACE TABLE user_performance (
id         INT64 NOT NULL,
name       STRING NOT NULL,
visited    TIMESTAMP,
perf       STRUCT<
rank INT64,
score FLOAT64
>,
quiz ARRAY<STRUCT<
key string,
value STRUCT<
id INT64,
score INT64,
taken TIMESTAMP
>>>
);
