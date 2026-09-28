CREATE TABLE ddb_stream_records (
    table_id TEXT NOT NULL,
    stream_label TEXT NOT NULL,
    sequence_number TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    record BLOB NOT NULL,
    PRIMARY KEY (table_id, stream_label, sequence_number)
);
CREATE INDEX ddb_stream_records_expiry ON ddb_stream_records (created_at_ms);
