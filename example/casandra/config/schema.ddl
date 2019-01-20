DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id       float PRIMARY KEY,
  username text ,
  active   boolean,
  salary   float,
  comments text,
  modified TIMESTAMP,
  zz Counter
);