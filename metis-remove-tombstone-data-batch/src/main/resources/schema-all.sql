DROP TABLE entry IF EXISTS;

CREATE TABLE entry
(
    tombstone_id  BIGINT IDENTITY NOT NULL PRIMARY KEY,
    europeana_id VARCHAR(300),
    resource_url  VARCHAR(512),
    status        VARCHAR(10)
);
