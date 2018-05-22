-- ECB exchange rates are given at the end of the day and only for bank working days.
-- Make sure there are exchange rates are there for every date and currency

INSERT INTO euro_fx.exchange_rate

  WITH all_days_and_currencies AS (
    -- cross product between all days and all currencies
      SELECT
        day :: DATE AS date,
        currency
      FROM
            generate_series((SELECT min(date)
                             FROM euro_fx.exchange_rate),
                            current_timestamp,
                            '1 day' :: INTERVAL) day
        CROSS JOIN (SELECT DISTINCT currency
                    FROM euro_fx.exchange_rate) currency),

      dates_of_last_known_exchange_rates AS (
      -- find for each currency and date the last date when an exchange rates was known
        SELECT
          date,
          currency,
          exchange_rate,
          max(date)
            FILTER (WHERE exchange_rate IS NOT NULL)
          OVER (
            PARTITION BY currency
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS max_date_with_exchange_rate
        FROM euro_fx.exchange_rate
          RIGHT JOIN all_days_and_currencies USING (date, currency)),


      last_know_exchange_rates AS (
      -- find for each row the last known exchange rate
        SELECT
          date,
          currency,
          exchange_rate,
          min(exchange_rate)
          OVER (
            PARTITION BY currency, max_date_with_exchange_rate
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS last_exchange_rate
        FROM dates_of_last_known_exchange_rates)

  SELECT
    currency,
    last_exchange_rate,
    date
  FROM last_know_exchange_rates
  WHERE exchange_rate IS NULL AND last_exchange_rate IS NOT NULL
  ORDER BY currency, date;

-- Insert an exchange rate of 1 for 'EUR' to simplify subsequent joins
INSERT INTO euro_fx.exchange_rate
  SELECT
    'EUR' AS currency,
    1     AS exchange_rate,
    date
  FROM (
         SELECT DISTINCT date
         FROM euro_fx.exchange_rate
       ) t
  ORDER BY date;

ANALYZE euro_fx.exchange_rate;

CREATE INDEX exchange_rate__currency_date
  ON euro_fx.exchange_rate (currency, date) WITH ( FILLFACTOR = 100 );

