-- functions for interacting with the cstore foreign data wrapper (https://github.com/citusdata/cstore_fdw)


-- requires manual installation of extension
CREATE EXTENSION IF NOT EXISTS cstore_fdw;

-- create cstore server
DO $$
BEGIN
  IF NOT (SELECT exists(SELECT 1
                        FROM pg_foreign_server
                        WHERE srvname = 'cstore_server'))
  THEN CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
  END IF;
END$$;


-- creates a cstore fdw table from a view
CREATE OR REPLACE FUNCTION util.create_cstore_fdw_table(_table_schema TEXT, _table_name TEXT, _view_schema TEXT,
                                                        _view_name    TEXT)
  RETURNS VOID AS $$
DECLARE ddl TEXT;
BEGIN
  SELECT string_agg('"' || column_name || '" ' || data_type, E',\n ')
  FROM (SELECT *
        FROM information_schema.columns
        WHERE table_schema = _view_schema AND table_name = _view_name
        ORDER BY ordinal_position) t
  INTO ddl;
  EXECUTE 'CREATE FOREIGN TABLE ' || _table_schema || '.' || _table_name || '(' || ddl || ') SERVER cstore_server
OPTIONS(compression ''pglz'');';
END;
$$ LANGUAGE plpgsql;

-- creates a cstore fdw table as a partition of a normal table
-- (so that the cstore table is visible in database clients that can't process foreign tables)
CREATE OR REPLACE FUNCTION util.create_cstore_partition(schema_name TEXT, table_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'CREATE FOREIGN TABLE ' || schema_name || '.' || table_name || '_cstore () '
          || ' INHERITS(' || schema_name || '.' || table_name
          || ') SERVER cstore_server OPTIONS (compression ''pglz'')';
END;
$$ LANGUAGE plpgsql;
