/** Functions for adding constraints and indexes that also work with partitioned tables */


/** A helper function for retrieving all inherited tables of a table */
CREATE OR REPLACE FUNCTION util.get_inherited_tables(schema_name          TEXT,
                                                     table_name           TEXT,
  OUT                                                inherited_table_name TEXT)
  RETURNS SETOF TEXT AS $$
BEGIN
  RETURN QUERY
  SELECT inherited_table.relname :: TEXT AS tname
  FROM pg_inherits
    JOIN pg_class inherited_table ON pg_inherits.inhrelid = inherited_table.oid
    JOIN pg_class parent_table ON pg_inherits.inhparent = parent_table.oid
    JOIN pg_namespace parent_schema ON parent_table.relnamespace = parent_schema.oid
  WHERE parent_schema.nspname = schema_name AND parent_table.relname = table_name;
END;
$$
LANGUAGE 'plpgsql';


/** Retrieves all columns of a table following a pattern */
CREATE OR REPLACE FUNCTION util.get_columns(schema_name TEXT, table_name TEXT, pattern TEXT DEFAULT '%')
  RETURNS SETOF TEXT AS $$
BEGIN
  RETURN QUERY EXECUTE '
SELECT column_name::TEXT
FROM information_schema.columns
WHERE table_catalog = (SELECT * from information_schema.information_schema_catalog_name)
    AND table_schema = ' || quote_literal($1) || '
    AND columns.table_name = ' || quote_literal($2) || '
    AND column_name LIKE ' || quote_literal($3);
END;
$$
LANGUAGE 'plpgsql';


/**
Adds an index to a table.

If the table has inherited tables, then the function is called on each child table

Parameters:
  partition_id: used to avoid concatenating the _## to the table name in an ugly manner
  column_names: a list of columns
  expression: a function for a function index
  replace_: whether to replace existing index
  unique_: when true, then create a unique index
  method: btree, hash or brin
  partial_condition: an SQL expression that returns a boolean value. When it evaluates to true for the row,
         the row is included in the index. Makes for smaller indexes (good for RAM and disk, and speed)
  index_name: pass a name for the index, for long indexes that we might want to cluster on
  analyze_: whether to perform an ANALYZE of the table after adding the index
  cluster_: whether to cluster on the newly created index
*/
CREATE OR REPLACE FUNCTION util.add_index(schema_name       TEXT,
                                          table_name        TEXT,
                                          partition_id      SMALLINT DEFAULT NULL,
                                          column_names      TEXT [] DEFAULT NULL,
                                          expression        TEXT DEFAULT NULL,
                                          replace_          BOOLEAN DEFAULT FALSE,
                                          unique_           BOOLEAN DEFAULT FALSE,
                                          method            TEXT DEFAULT 'btree',
                                          partial_condition TEXT DEFAULT NULL,
                                          index_name        TEXT DEFAULT NULL,
                                          analyze_          BOOLEAN DEFAULT FALSE,
                                          cluster_          BOOLEAN DEFAULT FALSE)
  RETURNS VOID AS $$
DECLARE
  inherited_tables TEXT [];
  inherited_table  TEXT;
  ddl              TEXT;
BEGIN
  IF column_names IS NULL AND expression IS NULL
  THEN
    RAISE EXCEPTION 'please provide either param :column_names or :expression';
  END IF;

  IF partition_id IS NOT NULL
  THEN
    table_name := table_name || '_' || partition_id;
  END IF;

  SELECT INTO inherited_tables array_agg(inherited_table_name)
  FROM util.get_inherited_tables(schema_name, table_name);

  IF array_length(inherited_tables, 1) > 0
  THEN
    FOREACH inherited_table IN ARRAY inherited_tables LOOP
      PERFORM util.add_index(schema_name, inherited_table, partition_id, column_names, expression, replace_, unique_,
                             method, partial_condition, index_name, analyze_);
    END LOOP;
  ELSE

    ddl := 'CREATE';
    IF unique_
    THEN
      ddl := ddl || ' UNIQUE';
    END IF;

    IF index_name IS NULL
    THEN
      index_name := table_name || '__';
      IF column_names IS NOT NULL
      THEN index_name := index_name || array_to_string(column_names, '_');
      ELSE index_name := index_name || expression;
      END IF;
    END IF;

    ddl := ddl || ' INDEX ';
    IF replace_
    THEN
      EXECUTE 'DROP INDEX IF EXISTS "' || schema_name || '"."' || index_name || '";';
    ELSE
      ddl := ddl || 'IF NOT EXISTS ';
    END IF;

    ddl := ddl || '"' || index_name || '"';

    ddl := ddl || ' ON "' || schema_name || '"."' || table_name || '" USING ' || method || ' (';

    IF column_names IS NOT NULL
    THEN ddl := ddl || '"' || array_to_string(column_names, '", "') || '"';
    ELSE ddl := ddl || expression;
    END IF;

    ddl := ddl || ')';

    IF method IN ('btree', 'hash')
    THEN ddl := ddl || ' WITH (FILLFACTOR=100)';
    END IF;

    IF partial_condition IS NOT NULL
    THEN ddl := ddl || ' WHERE ' || partial_condition;
    END IF;

    ddl := ddl || ';';

    EXECUTE ddl;

    IF cluster_
    THEN
      EXECUTE 'CLUSTER "' || schema_name || '"."' || table_name || '" USING "' || index_name || '";';
    END IF;

    IF analyze_
    THEN
      EXECUTE 'ANALYZE "' || schema_name || '"."' || table_name || '";';
    END IF;
  END IF;
END;
$$
LANGUAGE 'plpgsql';


/** Adds indexes to all foreign keys of a table (column name = '*_fk') */
CREATE OR REPLACE FUNCTION util.add_indexes_on_all_fks(schema_name      TEXT,
                                                       table_name       TEXT,
                                                       method           TEXT DEFAULT 'brin',
                                                       excluded_columns TEXT [] DEFAULT NULL)
  RETURNS VOID AS $$
DECLARE
  column_name       TEXT;
  excluded_columns_ TEXT [];
BEGIN
  excluded_columns_ := coalesce(excluded_columns, ARRAY [] :: TEXT []);
  RAISE NOTICE 'excluded_columns_: %', excluded_columns_;
  FOR column_name IN
  SELECT util.get_columns(schema_name, table_name, '%_fk')
  LOOP
    IF NOT (excluded_columns_ @> ARRAY [column_name])
    THEN
      RAISE NOTICE 'Perform index on % with method %', column_name, method;
      PERFORM util.add_index(schema_name, table_name, column_names := ARRAY [column_name], method := method);
    END IF;
  END LOOP;
END;
$$
LANGUAGE 'plpgsql';


/**
Ads a primary key to a table

Assumes that the table has a <table_name>_id column that is the primary key
*/
CREATE FUNCTION util.add_pk(schema_name TEXT, table_name TEXT)
  RETURNS VOID AS $$
BEGIN
  EXECUTE 'ALTER TABLE ' || schema_name || '.' || table_name ||
          ' ADD PRIMARY KEY (' || table_name || '_id);';
END;
$$
LANGUAGE 'plpgsql';


/**
Adds a foreign key from one table to another table.

If the source table has inheritance children, then the function is called on each of them.
 */
CREATE OR REPLACE FUNCTION util.add_fk(source_schema_name TEXT,
                                       source_table_name  TEXT,
                                       source_column_name TEXT,
                                       target_schema_name TEXT,
                                       target_table_name  TEXT)
  RETURNS VOID AS $$
DECLARE
  inherited_tables TEXT [];
  inherited_table  TEXT;
BEGIN
  SELECT INTO inherited_tables array_agg(inherited_table_name)
  FROM util.get_inherited_tables(source_schema_name, source_table_name);

  IF array_length(inherited_tables, 1) > 0
  THEN FOREACH inherited_table IN ARRAY inherited_tables LOOP
    PERFORM util.add_fk(source_schema_name, inherited_table, source_column_name, target_schema_name, target_table_name);
  END LOOP;
  ELSE
    EXECUTE 'ALTER TABLE ' || source_schema_name || '.' || source_table_name ||
            ' ADD FOREIGN KEY (' || source_column_name || ')' ||
            ' REFERENCES ' || target_schema_name || '.' || target_table_name || ' (' || target_table_name || '_id);';
  END IF;
END;
$$
LANGUAGE 'plpgsql';


/**
Adds a foreign key from on table to another table

Assumes that if the other table is called foo, then for foreign key has to be named foo_fk and the primary key foo_id
 */
CREATE FUNCTION util.add_fk(source_schema_name TEXT, source_table_name TEXT,
                            target_schema_name TEXT, target_table_name TEXT)
  RETURNS VOID AS $$
SELECT util.add_fk(source_schema_name, source_table_name, target_table_name || '_fk',
                   target_schema_name, target_table_name);
$$
LANGUAGE SQL;


/**
Adds a check constraint to a table

  expression: textual representation of the check expression
*/
CREATE OR REPLACE FUNCTION util.add_check(schema_name  TEXT,
                                          table_name   TEXT,
                                          partition_id SMALLINT,
                                          name         TEXT,
                                          expression   TEXT)
  RETURNS VOID AS $$
DECLARE
  inherited_tables TEXT [];
  inherited_table  TEXT;
  ddl              TEXT;
BEGIN
  IF partition_id IS NOT NULL
  THEN
    table_name := table_name || '_' || partition_id;
  END IF;

  SELECT INTO inherited_tables array_agg(inherited_table_name)
  FROM util.get_inherited_tables(schema_name, table_name);

  IF array_length(inherited_tables, 1) > 0
  THEN
    FOREACH inherited_table IN ARRAY inherited_tables LOOP
      PERFORM util.add_check(schema_name, inherited_table, partition_id, name, expression);
    END LOOP;
  ELSE
    ddl := 'ALTER TABLE "' || schema_name || '"."' || table_name || '" ';
    ddl := ddl || 'ADD CONSTRAINT "' || name || '" CHECK (' || expression || ');';
    EXECUTE ddl;
  END IF;
END;
$$
LANGUAGE plpgsql;







