/** Functions for managing table partitions */


/** Adds for each key returned by key_query a native (Postgresql 10) partition to table_name */
CREATE OR REPLACE FUNCTION util.create_table_partitions(schemaname TEXT,
                                                        tablename  TEXT,
                                                        key_query  TEXT)
  RETURNS VOID AS $$

DECLARE key  INTEGER;
        keys INTEGER [];
BEGIN
  EXECUTE 'SELECT array(' || key_query || ')'
  INTO keys;
  FOREACH key IN ARRAY keys LOOP
    IF
    NOT EXISTS(SELECT 1
               FROM information_schema.tables t
               WHERE t.table_schema = schemaname
                     AND t.table_name = tablename || '_' || key)
    THEN
      EXECUTE 'CREATE TABLE ' || schemaname || '.' || tablename || '_' || key
              || ' PARTITION OF ' || schemaname || '.' || tablename
              || ' FOR VALUES IN ( ' || key :: INTEGER || ');';
    END IF;
  END LOOP;
END
$$
LANGUAGE plpgsql;


/** Adds evenly-distributed table partitions by hash (introduced in Postgres 11) to a given table_name and schema_name */
CREATE OR REPLACE FUNCTION util.create_table_hash_partitions(schema_name TEXT,
                                                             table_name TEXT,
                                                             partitions INT)
    RETURNS VOID AS
$$
DECLARE
    i      INT;
    target INT;
BEGIN
    target := partitions - 1;
    FOR i IN 0..target
        LOOP
            EXECUTE ' CREATE TABLE ' || schema_name || '.' || table_name || '_' || i ||
                    ' PARTITION OF ' || schema_name || '.' || table_name ||
                    ' FOR VALUES WITH (MODULUS ' || partitions || ', REMAINDER ' || i || ')';

        END LOOP;
END ;
$$ LANGUAGE plpgsql;
