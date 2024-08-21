CREATE TABLE IF NOT EXISTS test_table (
    id TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS related_table (
    id TEXT PRIMARY KEY,
    test_id TEXT REFERENCES test_table(id),
    related_value TEXT
);

CREATE TABLE IF NOT EXISTS another_related_table (
    id TEXT PRIMARY KEY,
    related_id TEXT REFERENCES related_table(id),
    another_value TEXT
);