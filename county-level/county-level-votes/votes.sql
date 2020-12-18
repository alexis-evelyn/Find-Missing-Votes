CREATE TABLE `votes` (
  `county` bigint NOT NULL,
  `candidate` bigint NOT NULL,
  `votes` bigint,
  PRIMARY KEY (`county`,`candidate`),
  KEY `candidate` (`candidate`),
  KEY `county` (`county`),
  CONSTRAINT `FK_Candidate` FOREIGN KEY (`candidate`) REFERENCES `candidate` (`id`),
  CONSTRAINT `FK_County` FOREIGN KEY (`county`) REFERENCES `county` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
