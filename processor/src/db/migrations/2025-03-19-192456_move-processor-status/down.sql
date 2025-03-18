-- Modify column length
-- Drop existing constraints if needed
ALTER TABLE backfill_processor_status DROP CONSTRAINT IF EXISTS backfill_processor_status_pkey;

-- Modify the backfill_alias column
ALTER TABLE backfill_processor_status
ALTER COLUMN backfill_alias TYPE VARCHAR(50),
ALTER COLUMN backfill_alias SET NOT NULL;

-- Add unique constraint and primary key
ALTER TABLE backfill_processor_status ADD CONSTRAINT backfill_processor_status_pkey UNIQUE (backfill_alias);
ALTER TABLE backfill_processor_status ADD PRIMARY KEY (backfill_alias);

-- Ensure the public tables exist before restoring data
CREATE TABLE IF NOT EXISTS public.processor_status AS TABLE processor_metadata.processor_status WITH NO DATA;
CREATE TABLE IF NOT EXISTS public.ledger_infos AS TABLE processor_metadata.ledger_infos WITH NO DATA;

-- Restore data to public tables
INSERT INTO public.processor_status SELECT * FROM processor_metadata.processor_status;
INSERT INTO public.ledger_infos SELECT * FROM processor_metadata.ledger_infos;

-- Drop the tables in processor_metadata schema
DROP TABLE IF EXISTS processor_metadata.processor_status;
DROP TABLE IF EXISTS processor_metadata.ledger_infos;

-- Drop the schema (only if empty)
DROP SCHEMA IF EXISTS processor_metadata;