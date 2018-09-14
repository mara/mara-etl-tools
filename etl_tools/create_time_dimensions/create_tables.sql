DROP SCHEMA IF EXISTS time CASCADE;
CREATE SCHEMA time;


CREATE TABLE time.day (
  day_id           INTEGER PRIMARY KEY,
  day_name         TEXT     NOT NULL UNIQUE,
  year_id          SMALLINT NOT NULL,
  year_name        TEXT     NOT NULL,
  iso_year_id      SMALLINT NOT NULL,
  iso_year_name    TEXT     NOT NULL,
  quarter_id       SMALLINT NOT NULL,
  quarter_name     TEXT     NOT NULL,
  month_id         INTEGER  NOT NULL,
  month_name       TEXT     NOT NULL,
  week_id          INTEGER  NOT NULL,
  week_name        TEXT     NOT NULL,
  day_of_week_id   SMALLINT NOT NULL,
  day_of_week_name TEXT     NOT NULL,
  day_of_month_id  SMALLINT NOT NULL,
  _date            DATE     NOT NULL
);

SELECT util.add_index('time', 'day', column_names := ARRAY ['year_id']);
SELECT util.add_index('time', 'day', column_names := ARRAY ['iso_year_id']);
SELECT util.add_index('time', 'day', column_names := ARRAY ['quarter_id']);
SELECT util.add_index('time', 'day', column_names := ARRAY ['month_id']);
SELECT util.add_index('time', 'day', column_names := ARRAY ['week_id']);
SELECT util.add_index('time', 'day', column_names := ARRAY ['day_of_week_id']);
SELECT util.add_index('time', 'day', column_names := ARRAY ['day_of_month_id']);


CREATE TABLE time.duration (
  duration_id      INTEGER  NOT NULL,
  days             SMALLINT NOT NULL,
  days_name        TEXT     NOT NULL,
  weeks            SMALLINT NOT NULL,
  weeks_name       TEXT     NOT NULL,
  four_weeks       SMALLINT NOT NULL,
  four_weeks_name  TEXT     NOT NULL,
  months           SMALLINT NOT NULL,
  months_name      TEXT     NOT NULL,
  sixth_years      SMALLINT NOT NULL,
  sixth_years_name TEXT     NOT NULL,
  half_years       SMALLINT NOT NULL,
  half_years_name  TEXT     NOT NULL,
  years            SMALLINT NOT NULL,
  years_name       TEXT     NOT NULL
);

SELECT util.add_pk('time', 'duration');
SELECT util.add_index('time', 'duration', column_names := ARRAY ['days']);
SELECT util.add_index('time', 'duration', column_names := ARRAY ['weeks']);
SELECT util.add_index('time', 'duration', column_names := ARRAY ['four_weeks']);
SELECT util.add_index('time', 'duration', column_names := ARRAY ['months']);
SELECT util.add_index('time', 'duration', column_names := ARRAY ['sixth_years']);
SELECT util.add_index('time', 'duration', column_names := ARRAY ['half_years']);
SELECT util.add_index('time', 'duration', column_names := ARRAY ['years']);


CREATE TABLE time.hour_of_day (
  hour_of_day_id   SMALLINT PRIMARY KEY,
  hour_of_day_name TEXT NOT NULL UNIQUE
);

INSERT INTO time.hour_of_day
  SELECT
    h AS hour_of_day_id,
    h || '-' || h + 1
  FROM generate_series(0, 23) h;

--
-- compute all date values from start_date to now,
-- compute all durations for the date range
--
CREATE OR REPLACE FUNCTION time.populate_time_dimensions(start_date DATE, end_date DATE)
  RETURNS VOID AS $$

INSERT INTO time.day
  SELECT
    to_char(d, 'YYYYMMDD') :: INTEGER AS day_id,
    to_char(d, 'Dy, Mon DD YYYY')     AS day_name,
    extract('year' FROM d)            AS year_id,
    to_char(d, 'YYYY')                AS year_name,
    extract('isoyear' FROM d)         AS iso_year_id,
    extract('isoyear' FROM d) :: TEXT AS iso_year_name,
    to_char(d, 'YYYYQ') :: SMALLINT   AS quarter_id,
    to_char(d, 'YYYY "Q"Q')           AS quarter_name,
    to_char(d, 'YYYYMM') :: INTEGER   AS month_id,
    to_char(d, 'YYYY Mon')            AS month_name,
    to_char(d, 'IYYYIW') :: INTEGER   AS week_id,
    to_char(d, 'IYYY "-" "CW "IW')    AS week_name,
    to_char(d, 'ID') :: SMALLINT      AS day_of_week_id,
    to_char(d, 'Day')                 AS day_of_week_name,
    to_char(d, 'DD') :: SMALLINT      AS day_of_month_id,
    d                                 AS _date

  FROM generate_series($1 :: TIMESTAMP - INTERVAL '1 Day', $2 :: TIMESTAMP, '1 day') AS d
    LEFT JOIN time.day ON day._date = d
  WHERE day.day_id IS NULL;


INSERT INTO time.duration
  SELECT
    n - 1                                                           AS duration_id,
    n - 1                                                           AS days,
    '-' || -n || ' days'                                            AS days_name,
    floor((n - 7) / 7)                                              AS weeks,
    '-' || -n + n % 7 || ' to -' || -n + 6 + n % 7 || ' days'       AS weeks_name,
    floor((n - 28) / 28)                                            AS four_weeks,
    '-' || -n + n % 28 || ' to -' || -n + 27 + n % 28 || ' days'    AS four_weeks_name,
    floor((n - 30) / 30)                                            AS months,
    '-' || -n + n % 30 || ' to -' || -n + 29 + n % 30 || ' days'    AS months_name,
    floor((n - 60) / 60)                                            AS sixth_years,
    '-' || -n + n % 90 || ' to -' || -n + 59 + n % 60 || ' days'    AS sixth_years_name,
    floor((n - 180) / 180)                                          AS half_years,
    '-' || -n + n % 180 || ' to -' || -n + 179 + n % 180 || ' days' AS half_years_name,
    floor((n - 360) / 360)                                          AS years,
    '-' || -n + n % 360 || ' to -' || -n + 359 + n % 360 || ' days' AS years_name

  FROM generate_series($1 - current_date, 0, 1) AS n
ON CONFLICT DO NOTHING;


INSERT INTO time.duration
  SELECT
    n                                                  AS duration_id,
    n                                                  AS days,
    n || ' days'                                       AS days_name,
    floor(n / 7)                                       AS weeks,
    n - n % 7 || '-' || n + 6 - n % 7 || ' days'       AS weeks_name,
    floor(n / 28)                                      AS four_weeks,
    n - n % 28 || '-' || n + 27 - n % 28 || ' days'    AS four_weeks_name,
    floor(n / 30)                                      AS months,
    n - n % 30 || '-' || n + 29 - n % 30 || ' days'    AS months_name,
    floor(n / 60)                                      AS sixth_years,
    n - n % 60 || '-' || n + 59 - n % 60 || ' days'    AS sixth_years_name,
    floor(n / 180)                                     AS half_years,
    n - n % 180 || '-' || n + 179 - n % 180 || ' days' AS half_years_name,
    floor(n / 360)                                     AS years,
    n - n % 360 || '-' || n + 359 - n % 360 || ' days' AS years_name

  FROM generate_series(0, current_date - $1 + 2, 1) AS n
ON CONFLICT DO NOTHING;


INSERT INTO time.duration
  SELECT
    -30000   AS duration_id,
    -30000   AS days,
    'before' AS days_name,
    -30000   AS weeks,
    'before' AS weeks_name,
    -30000   AS four_weeks,
    'before' AS four_weeks_name,
    -30000   AS months,
    'before' AS months_name,
    -30000   AS sixth_years,
    'before' AS sixth_years_name,
    -30000   AS half_years,
    'before' AS half_years_naem,
    -30000   AS years,
    'before' AS years_name
ON CONFLICT DO NOTHING;


$$
LANGUAGE SQL;


