DROP TABLE IF EXISTS musers;

CREATE TABLE musers (
  id               INTEGER PRIMARY KEY,
  username         VARCHAR(255)       DEFAULT NULL,
  active           boolean            DEFAULT TRUE,
  salary           DECIMAL(7, 2)      DEFAULT NULL,
  comments         VARCHAR(255),
  modified          TIMESTAMP
);