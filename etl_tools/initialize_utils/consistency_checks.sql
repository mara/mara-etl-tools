/** Functions for checking the consistency and correctness of data */


/** Raises an exception when a boolean expression does not evaluate to t */
CREATE FUNCTION util.assert(description TEXT, query TEXT)
  RETURNS BOOLEAN AS $$
DECLARE
  succeeded BOOLEAN;
BEGIN
  EXECUTE query
  INTO succeeded;
  IF NOT succeeded
  THEN
    RAISE EXCEPTION 'assertion failed:
# % #
%', description, query;
  END IF;
  RETURN succeeded;
END
$$
LANGUAGE 'plpgsql';


/** raises an exception when the evaluation of two number-returning queries does not satisfy the given constraint */
CREATE FUNCTION util.assert_relation(description TEXT,
                                     query1      TEXT, query2 TEXT,
                                     relation    TEXT)
  RETURNS BOOLEAN AS $$
DECLARE
  result1   NUMERIC;
  result2   NUMERIC;
  succeeded BOOLEAN;
BEGIN
  EXECUTE query1
  INTO result1;
  EXECUTE query2
  INTO result2;
  EXECUTE 'SELECT ' || result1 || ' ' || relation || ' ' || result2
  INTO succeeded;
  IF NOT succeeded
  THEN
    RAISE EXCEPTION '%
assertion failed: % % %
%: (%)
%: (%)', description, result1, relation, result2, result1, query1, result2, query2;
  END IF;
  RETURN succeeded;
END
$$
LANGUAGE 'plpgsql';


/** raises an exception when the evaluation of two number-returning queries does not lead to the same result */
CREATE FUNCTION util.assert_equal(description TEXT, query1 TEXT, query2 TEXT)
  RETURNS BOOLEAN AS $$
BEGIN
  RETURN util.assert_relation(description, query1, query2, '=');
END
$$
LANGUAGE 'plpgsql';


/** raises an exception when the evaluation of two number-returning queries leads to the same result */
CREATE FUNCTION util.assert_not_equal(description TEXT, query1 TEXT, query2 TEXT)
  RETURNS BOOLEAN AS $$
BEGIN
  RETURN util.assert_relation(description, query1, query2, '!=');
END
$$
LANGUAGE 'plpgsql';


/** raises an exception when the evaluation of query 1 is bigger than the result of query 2 */
CREATE FUNCTION util.assert_smaller_than_or_equal(description TEXT, query1 TEXT, query2 TEXT)
  RETURNS BOOLEAN AS $$
BEGIN
  RETURN util.assert_relation(description, query1, query2, '<=');
END
$$
LANGUAGE 'plpgsql';


/** raises an exception when the evaluation of query is smaller than the result of query 2 */
CREATE FUNCTION util.assert_bigger_than_or_equal(description TEXT, query1 TEXT, query2 TEXT)
  RETURNS BOOLEAN AS $$
BEGIN
  RETURN util.assert_relation(description, query1, query2, '>=');
END
$$
LANGUAGE 'plpgsql';


/** raises an exception when the second query returns a value that is more than a percentage different than the first one */
CREATE FUNCTION util.assert_almost_equal(description TEXT,
                                         percentage  DECIMAL,
                                         query1      TEXT,
                                         query2      TEXT)
  RETURNS BOOLEAN AS $$
DECLARE
  result1   NUMERIC;
  result2   NUMERIC;
  succeeded BOOLEAN;
BEGIN
  EXECUTE query1
  INTO result1;
  EXECUTE query2
  INTO result2;
  EXECUTE 'SELECT coalesce( abs(' || result2 || ' - ' || result1 || ') / nullif(' || result1 || ', 0), 0 ) < ' ||
          percentage
  INTO succeeded;
  IF NOT succeeded
  THEN
    RAISE EXCEPTION '%
assertion failed: abs(% - %) - % < %
%: (%)
%: (%)', description, result2, result1, result1, percentage, result1, query1, result2, query2;
  END IF;
  RETURN succeeded;
END
$$
LANGUAGE 'plpgsql';


/**
 Takes a query that returns all rows that fail a consistency check.

 This function will fail if the query returns any results.
 */
CREATE OR REPLACE FUNCTION util.assert_not_found(description TEXT,
                                                 query       TEXT)
  RETURNS BOOLEAN AS $$
DECLARE
  result TEXT;
BEGIN

  EXECUTE 'SELECT string_agg( (SELECT string_agg( key || '' = '' || coalesce( value, ''null'' ), '', '' )
                               FROM json_each_text( row_to_json(q.*) ) ), chr(10))
           FROM (' || trim(query, '; ') || '
                 LIMIT 20 ) q'
  INTO result;

  IF result IS NOT NULL
  THEN
    RAISE EXCEPTION 'assertion %, failed for:
%', description, result;
  END IF;
  RETURN result IS NULL;
END
$$
LANGUAGE 'plpgsql';
