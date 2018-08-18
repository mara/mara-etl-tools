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


