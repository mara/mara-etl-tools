/** Initializes the HyperLogLog extension from https://github.com/citusdata/postgresql-hll */

CREATE EXTENSION IF NOT EXISTS hll;

DROP AGGREGATE IF EXISTS SUM(HLL);
CREATE AGGREGATE SUM(HLL) (
  SFUNC = hll_union,
  STYPE = HLL,
  FINALFUNC = hll_cardinality
);