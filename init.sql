
-- Insert region locations first
UPSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES
  ('region', 'us-east-1', 37.7749, -77.0369),
  ('region', 'us-west-2', 45.5231, -122.6765),
  ('region', 'us-central-1', 41.2565, -95.9345);

-- Enable vector index and buffered writes
SET CLUSTER SETTING feature.vector_index.enabled = true;
SET CLUSTER SETTING kv.transaction.write_buffering.enabled = true;

-- Create demo transactions table
CREATE TABLE IF NOT EXISTS defaultdb.demo_transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  amount INT NOT NULL
);

-- Note: For full multi-region setup, Banko AI app can run:

-- Set the primary region for the database (replace with a region returned by SHOW REGIONS FROM CLUSTER)
ALTER DATABASE defaultdb SET PRIMARY REGION "us-east-1";

-- Add additional regions
ALTER DATABASE defaultdb ADD REGION "us-west-2";
ALTER DATABASE defaultdb ADD REGION "us-central-1";

-- Add secondary region
ALTER DATABASE defaultdb SET SECONDARY REGION "us-west-2";

-- Optional: survive an entire region failure (requires 3+ database regions)
ALTER DATABASE defaultdb SURVIVE REGION FAILURE;

-- Verify database regions & survival goal
SHOW REGIONS FROM DATABASE defaultdb;