CREATE TABLE products_projection (
  generation TEXT NOT NULL,
  id BIGINT NOT NULL,
  document JSONB,
  deleted BOOLEAN NOT NULL,
  source_partition INTEGER NOT NULL,
  source_offset BIGINT NOT NULL,
  PRIMARY KEY(generation,id)
);
CREATE TABLE delivery_checkpoint (
  consumer_group TEXT NOT NULL,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  next_offset BIGINT NOT NULL,
  PRIMARY KEY(consumer_group,topic,partition)
);
CREATE TABLE delivery_history (
  consumer_group TEXT NOT NULL,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  offset_value BIGINT NOT NULL,
  PRIMARY KEY(consumer_group,topic,partition,offset_value)
);
CREATE TABLE consumer_binding (
  generation TEXT PRIMARY KEY,
  consumer_group TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL
);
