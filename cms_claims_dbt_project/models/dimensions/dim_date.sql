SELECT
    date AS date_key,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_of_week
FROM
    UNNEST(GENERATE_DATE_ARRAY('2008-01-01', '2010-12-31', INTERVAL 1 DAY)) AS date