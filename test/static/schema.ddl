DROP TABLE IF EXISTS users;

CREATE TABLE `users` (
  `id`               INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  `username`         VARCHAR(255)       DEFAULT NULL,
  `active`           TINYINT(1)         DEFAULT '1',
  `salary`           DECIMAL(7, 2)      DEFAULT NULL,
  `comments`         TEXT,
  `last_access_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS order_lines;
CREATE TABLE `order_lines` (
  `id`               INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  `order_id`         INT(11),
  `seq`              INT,
  `product_id`       INT(11),
  `product_price`    DECIMAL(7, 2),
  `quantity`         DECIMAL(7, 2),
  `create_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS products;
CREATE TABLE `products` (
  `id`          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  `name`       VARCHAR(255),
  `price`      DECIMAL(7, 2)
  );
