/** functions for dividing value ranges into chunks */


/**
Adds `compute_chunk` functions to a schema.

This is needed for building function indexes based on `compute_chunk`, because the index would be dropped if
the function gets dropped.

Example usage:

  DROP SCHEMA IF EXISTS xx_dim_next;
  CREATE SCHEMA xx_dim_next;
  SELECT util.create_chunking_functions('xx_dim');

 */
CREATE OR REPLACE FUNCTION util.create_chunking_functions(schema_name TEXT)
  RETURNS VOID AS $_$
BEGIN
  EXECUTE '
CREATE FUNCTION ' || schema_name || '.compute_chunk(x BIGINT)
  RETURNS SMALLINT AS $$
SELECT coalesce(abs(x) % number_of_chunks, 0) :: SMALLINT;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION ' || schema_name || '.compute_chunk(x TEXT)
  RETURNS SMALLINT AS $$
SELECT ' || schema_name || '.compute_chunk(abs((''x'' || substr(md5(x), 1, 8)) :: BIT(32) :: INT));
$$ LANGUAGE SQL IMMUTABLE;
';

END; $_$ LANGUAGE 'plpgsql';


-- add compute_chunk to utils schema
SELECT util.create_chunking_functions('util');


-- defined chunk ids as an array
CREATE FUNCTION util.get_all_chunks()
  RETURNS SETOF INTEGER AS $$
SELECT generate_series(0, number_of_chunks - 1);
$$ LANGUAGE SQL IMMUTABLE;



