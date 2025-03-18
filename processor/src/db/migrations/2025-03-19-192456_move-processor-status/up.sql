-- Recreate the procssor_metadata schemas
CREATE SCHEMA IF NOT EXISTS processor_metadata;

-- Tracks latest processed version per processor
CREATE TABLE IF NOT EXISTS processor_metadata.processor_status (
  processor VARCHAR(100) UNIQUE PRIMARY KEY NOT NULL,
  last_success_version BIGINT NOT NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  last_transaction_timestamp TIMESTAMP NULL
);

-- Tracks chain id
CREATE TABLE IF NOT EXISTS processor_metadata.ledger_infos (chain_id BIGINT UNIQUE PRIMARY KEY NOT NULL);

-- Copy data to processor_metadata tables
INSERT INTO processor_metadata.processor_status SELECT * FROM public.processor_status;
INSERT INTO processor_metadata.ledger_infos SELECT * FROM public.ledger_infos;

-- Modify column length
-- Drop existing constraints if needed
ALTER TABLE backfill_processor_status DROP CONSTRAINT IF EXISTS backfill_processor_status_pkey;

-- Modify the backfill_alias column
ALTER TABLE backfill_processor_status
ALTER COLUMN backfill_alias TYPE VARCHAR(100),
ALTER COLUMN backfill_alias SET NOT NULL;

-- Add unique constraint and primary key
ALTER TABLE backfill_processor_status ADD CONSTRAINT backfill_processor_status_pkey UNIQUE (backfill_alias);
ALTER TABLE backfill_processor_status ADD PRIMARY KEY (backfill_alias);