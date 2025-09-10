CREATE TABLE IF NOT EXISTS ogg_tables(
    extract_name TEXT,
    pdb_name     TEXT,
    schema_name  TEXT,
    table_name   TEXT,
    PRIMARY KEY (extract_name, pdb_name, schema_name, table_name)
);
CREATE TABLE IF NOT EXISTS ogg_table_stats_snap(
    run_time     DATETIME,
    extract_name TEXT,
    pdb_name     TEXT,
    schema_name  TEXT,
    table_name   TEXT,
    inserts      INTEGER,
    updates      INTEGER,
    deletes      INTEGER,
    PRIMARY KEY (run_time, extract_name, pdb_name, schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS idx_snap_time ON ogg_table_stats_snap(extract_name, run_time);
CREATE INDEX IF NOT EXISTS idx_snap_keys ON ogg_table_stats_snap(extract_name, pdb_name, schema_name, table_name, run_time);