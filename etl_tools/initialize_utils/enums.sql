/** Functions for creating enums from queries */


/** Adds a list of strings to an enum type */
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
BEGIN
  EXECUTE 'DROP TYPE IF EXISTS ' || enum_ || ' CASCADE';
  EXECUTE 'CREATE TYPE ' || enum_ || ' AS ENUM ();';

  PERFORM util.add_enum_values(enum_ :: REGTYPE, values_);
END;
$$
LANGUAGE plpgsql;
