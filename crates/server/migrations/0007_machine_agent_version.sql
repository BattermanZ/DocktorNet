-- The release each agent last reported in its snapshot.  NULL for machines
-- that pushed before this column existed and for agents that predate version
-- reporting.
ALTER TABLE machines ADD COLUMN agent_version TEXT;
