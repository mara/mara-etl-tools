/** creation of attribute lookup tables for flat data set tables */

-- needed to create indexes for auto-completion
CREATE EXTENSION IF NOT EXISTS pg_trgm;


/** creates a table optimized for the auto-completion of data set attributes */
CREATE OR REPLACE FUNCTION util.create_data_set_attributes_table(schema_name_ TEXT, table_name_ TEXT)
  RETURNS VOID AS $$
DECLARE column_name_ TEXT;
BEGIN
  EXECUTE 'DROP TABLE IF EXISTS ' || schema_name_ || '.' || table_name_ || '_attributes';
  EXECUTE 'CREATE TABLE ' || schema_name_ || '.' || table_name_ ||
          '_attributes (attribute TEXT NOT NULL, value TEXT NOT NULL, row_count BIGINT);';

  FOR column_name_ IN
  WITH enums AS (
      SELECT DISTINCT
        typname,
        nspname
      FROM pg_type
        JOIN pg_enum ON pg_type.oid = pg_enum.enumtypid
        JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
  )
  SELECT column_name
  FROM information_schema.columns
    LEFT JOIN enums ON udt_schema = enums.nspname AND udt_name = enums.typname
  WHERE table_schema = schema_name_
        AND table_name = table_name_
        AND (data_type IN ('text', 'varchar') OR enums.typname IS NOT NULL)

  LOOP
    EXECUTE 'INSERT INTO ' || schema_name_ || '.' || table_name_ || '_attributes ' ||
            'SELECT ''' || column_name_ || ''', "' || column_name_ ||
            '", count(*) FROM ' || schema_name_ || '.' || table_name_ ||
            ' WHERE "' || column_name_ || '" IS NOT NULL GROUP BY "' || column_name_
            || '" ORDER BY "' || column_name_ || '"';
  END LOOP;

  EXECUTE 'CREATE INDEX ' || table_name_ || '_attributes__attribute ON ' ||
          schema_name_ || '.' || table_name_ || '_attributes (attribute)';

  EXECUTE 'CREATE INDEX ' || table_name_ || '_attributes__value ON ' ||
          schema_name_ || '.' || table_name_ || '_attributes USING GIN (value gin_trgm_ops)';
END;
$$
LANGUAGE plpgsql;


-- the same as util.create_data_set_attributes_table but changed for parallel execution
-- before
CREATE OR REPLACE FUNCTION util.create_attributes_table_for_data_set(schema_name TEXT, table_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'DROP TABLE IF EXISTS ' || schema_name || '.' || table_name || '_attributes';
  EXECUTE 'CREATE TABLE ' || schema_name || '.' || table_name ||
          '_attributes (attribute TEXT NOT NULL, value TEXT NOT NULL);';
END;
$$
LANGUAGE plpgsql;

-- parallel
CREATE OR REPLACE FUNCTION util.insert_column_values_into_data_set_attributes_table(schema_name TEXT, table_name TEXT,
                                                                                    column_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'INSERT INTO ' || schema_name || '.' || table_name || '_attributes ' ||
          'SELECT DISTINCT ''' || column_name || ''', "' || column_name ||
          '" FROM ' || schema_name || '.' || table_name ||
          ' WHERE "' || column_name || '" IS NOT NULL ORDER BY "' || column_name || '"';
END;
$$
LANGUAGE plpgsql;

-- after
CREATE OR REPLACE FUNCTION util.index_data_set_attributes_table(schema_name TEXT, table_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'CREATE INDEX ' || table_name || '_attributes__attribute ON ' ||
          schema_name || '.' || table_name || '_attributes (attribute)';

  EXECUTE 'CREATE INDEX ' || table_name || '_attributes__value ON ' ||
          schema_name || '.' || table_name || '_attributes USING GIN (value gin_trgm_ops)';
END;
$$
LANGUAGE plpgsql;

-- parallized after function
CREATE OR REPLACE FUNCTION util.get_data_set_attributes(schema_name TEXT, table_name TEXT)
  RETURNS SETOF TEXT AS $$
BEGIN
  RETURN QUERY
  EXECUTE 'SELECT DISTINCT attribute FROM ' || schema_name || '.' || table_name || '_attributes';
END $$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION util.index_data_set_attributes_table(schema_name TEXT, table_name TEXT, attribute TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'CREATE INDEX "' || table_name || '_' || attribute || '" ON ' || schema_name || '.'
          || table_name || '_attributes USING GIN (value gin_trgm_ops) WHERE attribute = ''' || attribute || '''';
END
$$
LANGUAGE plpgsql;

