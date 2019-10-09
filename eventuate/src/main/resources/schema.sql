create table IF NOT EXISTS events (
  event_id varchar(1000) PRIMARY KEY,
  event_type varchar(1000),
  event_data varchar(1000) NOT NULL,
  entity_type VARCHAR(1000) NOT NULL,
  entity_id VARCHAR(1000) NOT NULL,
  triggering_event VARCHAR(1000),
  metadata VARCHAR(1000),
  published TINYINT DEFAULT 0,
  INDEX(entity_type, entity_id, event_id),
  INDEX(published, event_id)
);

create table IF NOT EXISTS entities (
  entity_type VARCHAR(1000),
  entity_id VARCHAR(1000),
  entity_version VARCHAR(1000) NOT NULL,
  PRIMARY KEY(entity_type, entity_id)
);

create table IF NOT EXISTS snapshots (
  entity_type VARCHAR(1000),
  entity_id VARCHAR(1000),
  entity_version VARCHAR(1000),
  snapshot_type VARCHAR(1000) NOT NULL,
  snapshot_json VARCHAR(1000) NOT NULL,
  triggering_events VARCHAR(1000),
  PRIMARY KEY(entity_type, entity_id, entity_version),
  INDEX(entity_type, entity_id)
);

create table IF NOT EXISTS cdc_monitoring (
  reader_id VARCHAR(1000) PRIMARY KEY,
  last_time BIGINT
);

CREATE TABLE IF NOT EXISTS message (
  id VARCHAR(767) PRIMARY KEY,
  destination VARCHAR(1000) NOT NULL,
  headers VARCHAR(1000) NOT NULL,
  payload VARCHAR(1000) NOT NULL,
  published SMALLINT DEFAULT 0,
  creation_time BIGINT,
  INDEX(published, id)
);

CREATE TABLE IF NOT EXISTS received_messages (
  consumer_id VARCHAR(767),
  message_id VARCHAR(767),
  PRIMARY KEY(consumer_id, message_id),
  creation_time BIGINT
);

CREATE TABLE IF NOT EXISTS eventuate.offset_store(
  client_name VARCHAR(255) NOT NULL PRIMARY KEY,
  serialized_offset VARCHAR(255)
);