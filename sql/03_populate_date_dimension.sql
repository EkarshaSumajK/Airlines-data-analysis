-- =====================================================
-- POPULATE DATE DIMENSION
-- =====================================================

-- Generate date dimension for 10 years (2020-2030)
WITH RECURSIVE date_range AS (
    SELECT DATE '2020-01-01'::DATE AS date_value
    UNION ALL
    SELECT (date_value + INTERVAL '1 day')::DATE
    FROM date_range
    WHERE date_value < DATE '2030-12-31'
)
INSERT INTO DimDate (
    DateKey,
    Date,
    DayOfWeek,
    DayOfMonth,
    Month,
    MonthName,
    Quarter,
    Year,
    HolidayFlag,
    IsWeekend,
    FiscalYear,
    FiscalQuarter
)
SELECT
    TO_CHAR(date_value, 'YYYYMMDD')::INT AS DateKey,
    date_value AS Date,
    TO_CHAR(date_value, 'Day') AS DayOfWeek,
    EXTRACT(DAY FROM date_value) AS DayOfMonth,
    EXTRACT(MONTH FROM date_value) AS Month,
    TO_CHAR(date_value, 'Month') AS MonthName,
    EXTRACT(QUARTER FROM date_value) AS Quarter,
    EXTRACT(YEAR FROM date_value) AS Year,
    CASE 
        WHEN TO_CHAR(date_value, 'MM-DD') IN ('01-01', '07-04', '12-25', '11-28') THEN TRUE
        ELSE FALSE
    END AS HolidayFlag,
    CASE 
        WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS IsWeekend,
    CASE 
        WHEN EXTRACT(MONTH FROM date_value) >= 10 THEN EXTRACT(YEAR FROM date_value) + 1
        ELSE EXTRACT(YEAR FROM date_value)
    END AS FiscalYear,
    CASE 
        WHEN EXTRACT(MONTH FROM date_value) IN (10, 11, 12) THEN 1
        WHEN EXTRACT(MONTH FROM date_value) IN (1, 2, 3) THEN 2
        WHEN EXTRACT(MONTH FROM date_value) IN (4, 5, 6) THEN 3
        ELSE 4
    END AS FiscalQuarter
FROM date_range;
