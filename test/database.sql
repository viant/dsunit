CREATE TABLE `users` (
  `id`               INT(11)   NOT NULL AUTO_INCREMENT,
  `username`         VARCHAR(255)       DEFAULT NULL,
  `active`           TINYINT(1)         DEFAULT '1',
  `salary`           DECIMAL(7, 2)      DEFAULT NULL,
  `comments`         TEXT,
  `last_access_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;



CREATE TABLE `order_lines` (
  `id`               INT(11)   NOT NULL AUTO_INCREMENT,
  `order_id`         INT(11),
  `seq`              INT,
  `product_id`       INT(11),
  `product_price`    DECIMAL(7, 2),
  `quantity`         DECIMAL(7, 2),
  `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;

CREATE TABLE `products` (
  `id`         INT(11)   NOT NULL AUTO_INCREMENT,
  `name`       VARCHAR(255),
  `price`      DECIMAL(7, 2),
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;
