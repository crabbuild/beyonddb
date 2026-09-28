CREATE TABLE ddb_stream_records (
    table_id TEXT NOT NULL,
    stream_label TEXT NOT NULL,
    sequence_number TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    record BLOB NOT NULL,
    PRIMARY KEY (table_id, stream_label, sequence_number)
);
CREATE INDEX ddb_stream_records_expiry ON ddb_stream_records (created_at_ms);

CREATE TABLE ddb_stream_catalog (
    table_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    stream_label TEXT NOT NULL,
    region TEXT NOT NULL,
    record BLOB NOT NULL,
    disabled_at_ms INTEGER,
    UNIQUE (table_name, stream_label)
);
CREATE INDEX ddb_stream_catalog_expiry ON ddb_stream_catalog (disabled_at_ms)
    WHERE disabled_at_ms IS NOT NULL;
