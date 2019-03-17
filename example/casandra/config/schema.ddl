DROP TABLE IF EXISTS users;

CREATE TABLE users
(
  id       int PRIMARY KEY,
  username text,
  active   boolean,
  comments text,
  modified text
);