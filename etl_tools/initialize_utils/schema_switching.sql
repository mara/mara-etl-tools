-- functions for replacing schemas with their next version


-- cancels all processes that hold any kind of lock on tables in "schema"
CREATE OR REPLACE FUNCTION util.cancel_queries_on_schema(schema TEXT)
  RETURNS BOOLEAN AS $$
SELECT pg_cancel_backend(pid)
FROM
  (SELECT DISTINCT pid
   FROM pg_locks
     JOIN pg_database ON database = pg_database.oid
     JOIN pg_class ON pg_class.oid = relation
     JOIN pg_namespace ON relnamespace = pg_namespace.oid
   WHERE datname = current_database() AND nspname = schema
         AND pid != pg_backend_pid()) t;
$$ LANGUAGE SQL;



-- replaces schema "schemaname" with "replace_with"
CREATE OR REPLACE FUNCTION util.replace_schema(schemaname TEXT, replace_with TEXT)
  RETURNS VOID AS $$
DECLARE foreign_table TEXT;
BEGIN
  PERFORM util.cancel_queries_on_schema(schemaname);

  IF EXISTS(SELECT *
            FROM information_schema.schemata s
            WHERE s.schema_name = schemaname)
  THEN
    EXECUTE 'ALTER SCHEMA ' || schemaname || ' RENAME TO ' || schemaname || '_old;';
  END IF;
  EXECUTE 'ALTER SCHEMA ' || replace_with || ' RENAME TO ' || schemaname || ';';

  -- again, for good measure
  PERFORM util.cancel_queries_on_schema(schemaname || '_old');

  EXECUTE 'DROP SCHEMA IF EXISTS ' || schemaname || '_old CASCADE;';

  -- since rights are dropped together with the replaced schema, they have to be added again
  PERFORM '';
END;
$$ LANGUAGE 'plpgsql';

