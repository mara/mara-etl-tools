/** Functions for creating enums from queries */


/**
Adds a list of strings to an enum type
Deprecated, only needed for PostgreSQL < 12
*/
CREATE OR REPLACE FUNCTION util.add_enum_values(type REGTYPE, values_ TEXT [])
  RETURNS VOID AS $$
DECLARE sortorder INTEGER;
        value     TEXT;
BEGIN
  SELECT coalesce(max(enumsortorder), 0) + 1
  FROM pg_enum
  WHERE enumtypid = type :: OID
  INTO sortorder;

  FOR value IN SELECT unnest(values_) AS value
  LOOP
    IF char_length(value) > 63
    THEN
      RAISE WARNING 'WARNING: Enum value should not be more than 63 characters length. Values are trimmed automatically. (%)', value;
    END IF;

    INSERT INTO pg_enum
    VALUES (type :: OID, sortorder, value);
    sortorder := sortorder + 1;
  END LOOP;
END;
$$
LANGUAGE plpgsql;




/** Creates an enum type with a list of values */
CREATE OR REPLACE FUNCTION util.create_enum(enum_ TEXT, values_ TEXT [])
  RETURNS VOID AS $$
DECLARE value      TEXT;
        pg_version INTEGER;
BEGIN
  EXECUTE 'DROP TYPE IF EXISTS ' || enum_ || ' CASCADE';
  EXECUTE 'CREATE TYPE ' || enum_ || ' AS ENUM ();';

  SELECT current_setting('server_version_num') INTO pg_version;

  -- Since PostgreSQL 12, it is possible to add enum values via ALTER TYPE .. ADD VALUE inside a transaction
  -- Before version 12, direct inserts into pg_enum were needed
  IF (pg_version >= 120000) THEN
      FOR value IN SELECT unnest(values_) AS value
      LOOP
         EXECUTE 'ALTER TYPE ' || enum_ || ' ADD VALUE ''' || value || '''';
      END LOOP;
  ELSE
      PERFORM util.add_enum_values(enum_ :: REGTYPE, values_);
  END IF;
END;
$$
LANGUAGE plpgsql;


--SELECT util.create_enum('util.enum_test', array['a', 'b', 'c']);
--SELECT * from pg_enum;
