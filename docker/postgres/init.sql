-- Each service owns its own schema. orderdb is created by POSTGRES_DB; this adds the second.
-- Runs only on first start, when the postgres-data volume is empty.
CREATE DATABASE inventorydb;
