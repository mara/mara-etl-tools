DROP SCHEMA IF EXISTS euro_fx CASCADE ;

CREATE SCHEMA euro_fx;

DROP TABLE IF EXISTS euro_fx.exchange_rate;

CREATE TABLE euro_fx.exchange_rate (
  currency      TEXT             NOT NULL,
  exchange_rate DOUBLE PRECISION NOT NULL,
  date          DATE             NOT NULL
);