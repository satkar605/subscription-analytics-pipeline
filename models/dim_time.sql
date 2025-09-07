MODEL (
    name dim.dates,
    kind VIEW,  -- Simple VIEW, not SCD_TYPE_2
    audits (
        not_null(columns := (date_key, full_date))
    )
);

-- Generate date range for our subscription data
WITH date_spine AS (
    SELECT 
        ('2022-08-01'::date + INTERVAL (seq) DAY)::date AS full_date
    FROM generate_series(0, 1186) AS t(seq)  -- 3.25 years (Aug 2022 - Oct 2025)
)

SELECT 
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    strftime(full_date, '%Y-%m-%d') AS date_string,
    EXTRACT(year FROM full_date) AS year,
    EXTRACT(quarter FROM full_date) AS quarter,
    EXTRACT(month FROM full_date) AS month,
    EXTRACT(day FROM full_date) AS day,
    EXTRACT(week FROM full_date) AS week_of_year,
    EXTRACT(dayofweek FROM full_date) AS day_of_week,
    strftime(full_date, '%A') AS day_name,
    strftime(full_date, '%B') AS month_name,
    strftime(full_date, '%Y-%m') AS year_month,
    CASE 
        WHEN EXTRACT(dayofweek FROM full_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend
FROM date_spine