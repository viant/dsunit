DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id       INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(255)  DEFAULT NULL,
  active   TINYINT(1)    DEFAULT TRUE,
  salary   DECIMAL(7, 2) DEFAULT NULL,
  comments TEXT,
  modified TIMESTAMP     DEFAULT current_timestamp
);