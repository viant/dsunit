DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id               SERIAL CONSTRAINT user_id PRIMARY KEY,
  username         VARCHAR(255)       DEFAULT NULL,
  active           boolean            DEFAULT TRUE,
  salary           DECIMAL(7, 2)      DEFAULT NULL,
  comments         TEXT,
  modified          TIMESTAMP DEFAULT current_timestamp
);