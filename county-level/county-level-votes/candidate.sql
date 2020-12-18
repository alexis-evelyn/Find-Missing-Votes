CREATE TABLE `candidate` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` longtext,
  `fec` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `fec` (`fec`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
