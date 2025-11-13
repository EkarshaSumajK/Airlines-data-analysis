-- =====================================================
-- DASHBOARD QUERIES FOR BI TOOLS
-- =====================================================

-- OPERATIONAL DASHBOARD
-- =====================================================

-- 1. Daily On-Time Performance (Last 30 Days)
CREATE OR REPLACE VIEW vw_daily_otp AS
SELECT 
    d.Date,
    COUNT(*) AS TotalFlights,
    SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) AS OnTimeFlights,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OTP,
    AVG(ff.ArrivalDelayMin) AS AvgDelay
FROM FactFlight ff
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '30 days'
  AND ff.CancellationFlag = FALSE
GROUP BY d.Date
ORDER BY d.Date;

-- 2. Delay Causes Summary
CREATE OR REPLACE VIEW vw_delay_causes AS
SELECT 
    'Weather' AS DelayType,
    COUNT(*) AS FlightCount,
    AVG(ff.ArrivalDelayMin) AS AvgDelayMin,
    SUM(ff.Revenue) AS ImpactedRevenue
FROM FactFlight ff
WHERE ff.WeatherImpactFlag = TRUE
  AND ff.ArrivalDelayMin > 15

UNION ALL

SELECT 
    'Mechanical',
    COUNT(*),
    AVG(ff.ArrivalDelayMin),
    SUM(ff.Revenue)
FROM FactFlight ff
WHERE ff.MechanicalDelayFlag = TRUE
  AND ff.ArrivalDelayMin > 15;

-- 3. Aircraft Utilization
CREATE OR REPLACE VIEW vw_aircraft_utilization AS
SELECT 
    ac.TailNumber,
    ac.Model,
    COUNT(DISTINCT ff.DateKey) AS DaysOperated,
    COUNT(*) AS TotalFlights,
    SUM(ff.AirTimeMin) / 60.0 AS TotalFlightHours,
    AVG(ff.LoadFactor) AS AvgLoadFactor
FROM FactFlight ff
JOIN DimAircraft ac ON ff.AircraftKey = ac.AircraftKey
WHERE ac.IsCurrent = TRUE
GROUP BY ac.TailNumber, ac.Model
ORDER BY TotalFlightHours DESC;

-- COMMERCIAL DASHBOARD
-- =====================================================

-- 4. Monthly Revenue Trends
CREATE OR REPLACE VIEW vw_monthly_revenue AS
SELECT 
    d.Year,
    d.MonthName,
    d.Month,
    SUM(ff.Revenue) AS FlightRevenue,
    SUM(fc.Revenue) AS CargoRevenue,
    SUM(ff.Revenue) + SUM(fc.Revenue) AS TotalRevenue,
    COUNT(DISTINCT ff.FlightFactKey) AS TotalFlights
FROM DimDate d
LEFT JOIN FactFlight ff ON d.DateKey = ff.DateKey
LEFT JOIN FactCargo fc ON d.DateKey = fc.DateKey
WHERE d.Year >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
GROUP BY d.Year, d.MonthName, d.Month
ORDER BY d.Year, d.Month;

-- 5. Route Performance Matrix
CREATE OR REPLACE VIEW vw_route_performance AS
SELECT 
    dep.IATA AS Origin,
    arr.IATA AS Destination,
    COUNT(*) AS Frequency,
    AVG(ff.LoadFactor) AS AvgLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue,
    ROUND(SUM(ff.Revenue) / NULLIF(SUM(ff.SeatsAvailable * ff.DistanceMiles), 0), 4) AS RASM,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OTP
FROM FactFlight ff
JOIN DimAirport dep ON ff.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON ff.ArrivalAirportKey = arr.AirportKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY dep.IATA, arr.IATA
HAVING COUNT(*) >= 10
ORDER BY TotalRevenue DESC;

-- 6. Fare Class Mix
CREATE OR REPLACE VIEW vw_fare_class_mix AS
SELECT 
    fc.CabinClass,
    fc.ClassName,
    COUNT(*) AS Bookings,
    SUM(fb.TotalAmount) AS Revenue,
    AVG(fb.TicketPrice) AS AvgFare,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS BookingShare,
    ROUND(100.0 * SUM(fb.TotalAmount) / SUM(SUM(fb.TotalAmount)) OVER (), 2) AS RevenueShare
FROM FactBooking fb
JOIN DimFareClass fc ON fb.FareClassKey = fc.FareClassKey
JOIN DimDate d ON fb.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '30 days'
  AND fb.CancellationFlag = FALSE
GROUP BY fc.CabinClass, fc.ClassName
ORDER BY Revenue DESC;

-- FINANCIAL DASHBOARD
-- =====================================================

-- 7. Route Profitability
CREATE OR REPLACE VIEW vw_route_profitability AS
SELECT 
    dep.City || ' - ' || arr.City AS Route,
    c.CarrierName,
    COUNT(*) AS Flights,
    SUM(ff.Revenue) AS Revenue,
    SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees) AS Costs,
    SUM(ff.Revenue) - SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees) AS Profit,
    ROUND((SUM(ff.Revenue) - SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees)) / 
          NULLIF(SUM(ff.Revenue), 0) * 100, 2) AS ProfitMargin
FROM FactFlight ff
JOIN DimFlight f ON ff.FlightKey = f.FlightKey
JOIN DimAirport dep ON ff.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON ff.ArrivalAirportKey = arr.AirportKey
JOIN DimCarrier c ON ff.CarrierKey = c.CarrierKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY Route, c.CarrierName
HAVING COUNT(*) >= 5
ORDER BY Profit DESC;

-- 8. Cost Breakdown
CREATE OR REPLACE VIEW vw_cost_breakdown AS
SELECT 
    d.Year,
    d.Quarter,
    SUM(ff.FuelCost) AS FuelCost,
    SUM(ff.CrewCost) AS CrewCost,
    SUM(ff.AirportFees) AS AirportFees,
    SUM(fm.TotalCost) AS MaintenanceCost,
    SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees) + SUM(fm.TotalCost) AS TotalCost
FROM DimDate d
LEFT JOIN FactFlight ff ON d.DateKey = ff.DateKey
LEFT JOIN FactMaintenance fm ON d.DateKey = fm.DateKey
WHERE d.Year >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
GROUP BY d.Year, d.Quarter
ORDER BY d.Year, d.Quarter;

-- 9. Maintenance Impact
CREATE OR REPLACE VIEW vw_maintenance_impact AS
SELECT 
    ac.Manufacturer,
    ac.Model,
    COUNT(*) AS MaintenanceEvents,
    SUM(fm.TotalCost) AS TotalCost,
    SUM(fm.DowntimeHours) AS TotalDowntime,
    SUM(fm.FlightsAffected) AS FlightsAffected,
    AVG(fm.TotalCost) AS AvgCostPerEvent
FROM FactMaintenance fm
JOIN DimAircraft ac ON fm.AircraftKey = ac.AircraftKey
JOIN DimDate d ON fm.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '180 days'
  AND ac.IsCurrent = TRUE
GROUP BY ac.Manufacturer, ac.Model
ORDER BY TotalCost DESC;

-- CUSTOMER ANALYTICS
-- =====================================================

-- 10. Loyalty Tier Performance
CREATE OR REPLACE VIEW vw_loyalty_performance AS
SELECT 
    c.LoyaltyTier,
    COUNT(DISTINCT c.CustomerKey) AS Customers,
    COUNT(fb.BookingKey) AS Bookings,
    SUM(fb.TotalAmount) AS Revenue,
    AVG(fb.TotalAmount) AS AvgBookingValue,
    ROUND(COUNT(fb.BookingKey)::NUMERIC / COUNT(DISTINCT c.CustomerKey), 2) AS BookingsPerCustomer
FROM DimCustomer c
LEFT JOIN FactBooking fb ON c.CustomerKey = fb.CustomerKey
WHERE c.IsCurrent = TRUE
  AND (fb.DateKey IS NULL OR fb.DateKey >= (SELECT DateKey FROM DimDate WHERE Date >= CURRENT_DATE - INTERVAL '365 days' LIMIT 1))
GROUP BY c.LoyaltyTier
ORDER BY Revenue DESC;

-- 11. Customer Retention
CREATE OR REPLACE VIEW vw_customer_retention AS
SELECT 
    d.Year,
    d.Quarter,
    COUNT(DISTINCT fb.CustomerKey) AS ActiveCustomers,
    COUNT(DISTINCT CASE WHEN repeat.CustomerKey IS NOT NULL THEN fb.CustomerKey END) AS ReturningCustomers,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN repeat.CustomerKey IS NOT NULL THEN fb.CustomerKey END) / 
          NULLIF(COUNT(DISTINCT fb.CustomerKey), 0), 2) AS RetentionRate
FROM FactBooking fb
JOIN DimDate d ON fb.DateKey = d.DateKey
LEFT JOIN (
    SELECT DISTINCT CustomerKey
    FROM FactBooking
    WHERE DateKey < (SELECT MIN(DateKey) FROM DimDate WHERE Year = EXTRACT(YEAR FROM CURRENT_DATE))
) repeat ON fb.CustomerKey = repeat.CustomerKey
WHERE d.Year >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
GROUP BY d.Year, d.Quarter
ORDER BY d.Year, d.Quarter;

-- KPI SUMMARY
-- =====================================================

-- 12. Executive KPI Dashboard
CREATE OR REPLACE VIEW vw_executive_kpis AS
SELECT 
    'Last 30 Days' AS Period,
    COUNT(DISTINCT ff.FlightFactKey) AS TotalFlights,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(*), 0), 2) AS OnTimePerformance,
    ROUND(AVG(ff.LoadFactor), 2) AS AvgLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue,
    ROUND(SUM(ff.Revenue) / NULLIF(SUM(ff.SeatsAvailable * ff.DistanceMiles), 0), 4) AS RASM,
    COUNT(DISTINCT fb.CustomerKey) AS ActiveCustomers,
    COUNT(DISTINCT fb.BookingKey) AS TotalBookings
FROM FactFlight ff
LEFT JOIN FactBooking fb ON ff.FlightKey = fb.FlightKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Date >= CURRENT_DATE - INTERVAL '30 days';
