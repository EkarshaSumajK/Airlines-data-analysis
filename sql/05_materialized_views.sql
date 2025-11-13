-- =====================================================
-- MATERIALIZED VIEWS FOR PERFORMANCE OPTIMIZATION
-- =====================================================

-- Monthly Flight Summary
CREATE MATERIALIZED VIEW mv_monthly_flight_summary AS
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    c.CarrierName,
    COUNT(*) AS TotalFlights,
    SUM(ff.SeatsFilled) AS TotalPassengers,
    SUM(ff.SeatsAvailable) AS TotalSeats,
    ROUND(AVG(ff.LoadFactor), 2) AS AvgLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue,
    SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees) AS TotalCosts,
    SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) AS OnTimeFlights,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OTP,
    SUM(CASE WHEN ff.CancellationFlag THEN 1 ELSE 0 END) AS Cancellations
FROM FactFlight ff
JOIN DimDate d ON ff.DateKey = d.DateKey
JOIN DimCarrier c ON ff.CarrierKey = c.CarrierKey
GROUP BY d.Year, d.Month, d.MonthName, c.CarrierName;

CREATE INDEX idx_mv_monthly_year_month ON mv_monthly_flight_summary(Year, Month);

-- Route Performance Summary
CREATE MATERIALIZED VIEW mv_route_performance AS
SELECT 
    dep.IATA AS OriginIATA,
    arr.IATA AS DestinationIATA,
    dep.City AS OriginCity,
    arr.City AS DestinationCity,
    c.CarrierName,
    COUNT(*) AS TotalFlights,
    AVG(ff.DistanceMiles) AS AvgDistance,
    AVG(ff.LoadFactor) AS AvgLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue,
    ROUND(SUM(ff.Revenue) / NULLIF(SUM(ff.SeatsAvailable * ff.DistanceMiles), 0), 4) AS RASM,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OTP,
    AVG(ff.ArrivalDelayMin) AS AvgDelay
FROM FactFlight ff
JOIN DimAirport dep ON ff.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON ff.ArrivalAirportKey = arr.AirportKey
JOIN DimCarrier c ON ff.CarrierKey = c.CarrierKey
GROUP BY dep.IATA, arr.IATA, dep.City, arr.City, c.CarrierName
HAVING COUNT(*) >= 10;

CREATE INDEX idx_mv_route_origin ON mv_route_performance(OriginIATA);
CREATE INDEX idx_mv_route_dest ON mv_route_performance(DestinationIATA);

-- Customer Lifetime Value
CREATE MATERIALIZED VIEW mv_customer_ltv AS
SELECT 
    c.CustomerKey,
    c.CustomerID,
    c.LoyaltyTier,
    COUNT(fb.BookingKey) AS TotalBookings,
    SUM(fb.TotalAmount) AS LifetimeRevenue,
    AVG(fb.TotalAmount) AS AvgBookingValue,
    MIN(fb.BookingDate) AS FirstBooking,
    MAX(fb.BookingDate) AS LastBooking,
    EXTRACT(DAYS FROM (MAX(fb.BookingDate) - MIN(fb.BookingDate))) AS CustomerTenureDays,
    SUM(fb.LoyaltyPointsEarned) AS TotalPointsEarned,
    SUM(CASE WHEN fb.CancellationFlag THEN 1 ELSE 0 END) AS Cancellations,
    ROUND(100.0 * SUM(CASE WHEN fb.CancellationFlag THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(fb.BookingKey), 0), 2) AS CancellationRate
FROM DimCustomer c
LEFT JOIN FactBooking fb ON c.CustomerKey = fb.CustomerKey
WHERE c.IsCurrent = TRUE
GROUP BY c.CustomerKey, c.CustomerID, c.LoyaltyTier;

CREATE INDEX idx_mv_customer_ltv_tier ON mv_customer_ltv(LoyaltyTier);

-- Aircraft Utilization Summary
CREATE MATERIALIZED VIEW mv_aircraft_utilization AS
SELECT 
    ac.AircraftKey,
    ac.TailNumber,
    ac.Manufacturer,
    ac.Model,
    ac.Age,
    COUNT(DISTINCT ff.DateKey) AS DaysOperated,
    COUNT(*) AS TotalFlights,
    SUM(ff.AirTimeMin) / 60.0 AS TotalFlightHours,
    AVG(ff.LoadFactor) AS AvgLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue,
    COUNT(fm.MaintenanceKey) AS MaintenanceEvents,
    SUM(fm.TotalCost) AS MaintenanceCosts,
    SUM(fm.DowntimeHours) AS TotalDowntime
FROM DimAircraft ac
LEFT JOIN FactFlight ff ON ac.AircraftKey = ff.AircraftKey
LEFT JOIN FactMaintenance fm ON ac.AircraftKey = fm.AircraftKey
WHERE ac.IsCurrent = TRUE
GROUP BY ac.AircraftKey, ac.TailNumber, ac.Manufacturer, ac.Model, ac.Age;

CREATE INDEX idx_mv_aircraft_tail ON mv_aircraft_utilization(TailNumber);

-- Daily Operations Summary (Rolling 90 days)
CREATE MATERIALIZED VIEW mv_daily_operations AS
SELECT 
    d.Date,
    d.DayOfWeek,
    d.IsWeekend,
    COUNT(*) AS TotalFlights,
    SUM(ff.SeatsFilled) AS TotalPassengers,
    AVG(ff.LoadFactor) AS AvgLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue,
    SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) AS OnTimeFlights,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OTP,
    AVG(ff.ArrivalDelayMin) AS AvgDelay,
    SUM(CASE WHEN ff.CancellationFlag THEN 1 ELSE 0 END) AS Cancellations,
    SUM(CASE WHEN ff.WeatherImpactFlag THEN 1 ELSE 0 END) AS WeatherDelays,
    SUM(CASE WHEN ff.MechanicalDelayFlag THEN 1 ELSE 0 END) AS MechanicalDelays
FROM FactFlight ff
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY d.Date, d.DayOfWeek, d.IsWeekend;

CREATE INDEX idx_mv_daily_ops_date ON mv_daily_operations(Date);

-- Refresh Scripts
-- Run these periodically (e.g., daily at 2 AM)

-- Refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_flight_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_route_performance;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_ltv;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_aircraft_utilization;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_operations;
    
    RAISE NOTICE 'All materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- Schedule refresh (example using pg_cron extension)
-- SELECT cron.schedule('refresh-mvs', '0 2 * * *', 'SELECT refresh_all_materialized_views()');
