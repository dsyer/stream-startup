CREATE TABLE IF NOT EXISTS offsets (
  topic VARCHAR(30),
  part INT,
  offset INT,
  PRIMARY KEY (topic, part)
) engine=InnoDB;
