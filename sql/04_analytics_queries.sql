-- =====================================================
-- AIRLINE ANALYTICS - KEY QUERIES
-- =====================================================

-- 1. ON-TIME PERFORMANCE BY ROUTE
SELECT 
    dep.IATA AS DepartureAirport,
    arr.IATA AS ArrivalAirport,
    c.CarrierName,
    COUNT(*) AS TotalFlights,
    SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) AS OnTimeFlights,
    ROUND(100.0 * SUM(CASE WHEN ff.ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OnTimePercentage,
    AVG(ff.ArrivalDelayMin) AS AvgArrivalDelay,
    AVG(ff.DepartureDelayMin) AS AvgDepartureDelay
FROM FactFlight ff
JOIN DimFlight f ON ff.FlightKey = f.FlightKey
JOIN DimAirport dep ON ff.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON ff.ArrivalAirportKey = arr.AirportKey
JOIN DimCarrier c ON ff.CarrierKey = c.CarrierKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Year = 2024
  AND ff.CancellationFlag = FALSE
GROUP BY dep.IATA, arr.IATA, c.CarrierName
HAVING COUNT(*) >= 10
ORDER BY OnTimePercentage DESC;

-- 2. REVENUE PER AVAILABLE SEAT MILE (RASM) BY ROUTE
SELECT 
    dep.City AS DepartureCity,
    arr.City AS ArrivalCity,
    d.Year,
    d.Quarter,
    SUM(ff.Revenue) AS TotalRevenue,
    SUM(ff.SeatsAvailable * ff.DistanceMiles) AS AvailableSeatMiles,
    ROUND(SUM(ff.Revenue) / NULLIF(SUM(ff.SeatsAvailable * ff.DistanceMiles), 0), 4) AS RASM,
    AVG(ff.LoadFactor) AS AvgLoadFactor
FROM FactFlight ff
JOIN DimAirport dep ON ff.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON ff.ArrivalAirportKey = arr.AirportKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Year >= 2023
GROUP BY dep.City, arr.City, d.Year, d.Quarter
ORDER BY d.Year DESC, d.Quarter DESC, RASM DESC;

-- 3. LOAD FACTOR AND YIELD BY FARE CLASS
SELECT 
    fc.ClassName,
    fc.CabinClass,
    d.Year,
    d.MonthName,
    COUNT(DISTINCT fb.BookingKey) AS TotalBookings,
    SUM(fb.TotalAmount) AS TotalRevenue,
    AVG(fb.TicketPrice) AS AvgTicketPrice,
    SUM(fb.TotalAmount) / NULLIF(COUNT(DISTINCT fb.BookingKey), 0) AS YieldPerBooking
FROM FactBooking fb
JOIN DimFareClass fc ON fb.FareClassKey = fc.FareClassKey
JOIN DimDate d ON fb.DateKey = d.DateKey
WHERE fb.CancellationFlag = FALSE
  AND d.Year = 2024
GROUP BY fc.ClassName, fc.CabinClass, d.Year, d.MonthName
ORDER BY d.Year, d.Month, TotalRevenue DESC;

-- 4. WEATHER IMPACT ON ON-TIME PERFORMANCE
SELECT 
    w.WeatherCondition,
    w.Severity,
    a.City AS Airport,
    COUNT(*) AS AffectedFlights,
    AVG(ff.ArrivalDelayMin) AS AvgDelay,
    SUM(CASE WHEN ff.CancellationFlag THEN 1 ELSE 0 END) AS Cancellations,
    SUM(ff.Revenue) AS LostRevenue
FROM FactFlight ff
JOIN DimWeather w ON ff.WeatherKey = w.WeatherKey
JOIN DimAirport a ON ff.DepartureAirportKey = a.AirportKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE ff.WeatherImpactFlag = TRUE
  AND d.Year = 2024
GROUP BY w.WeatherCondition, w.Severity, a.City
ORDER BY AffectedFlights DESC;

-- 5. AIRCRAFT MAINTENANCE COSTS AND RELIABILITY
SELECT 
    ac.Manufacturer,
    ac.Model,
    ac.Age,
    COUNT(DISTINCT fm.MaintenanceKey) AS MaintenanceEvents,
    SUM(fm.TotalCost) AS TotalMaintenanceCost,
    AVG(fm.TotalCost) AS AvgCostPerEvent,
    SUM(fm.DowntimeHours) AS TotalDowntimeHours,
    SUM(fm.FlightsAffected) AS TotalFlightsAffected,
    SUM(fm.TotalCost) / NULLIF(COUNT(DISTINCT ff.FlightFactKey), 0) AS CostPerFlightHour
FROM DimAircraft ac
JOIN FactMaintenance fm ON ac.AircraftKey = fm.AircraftKey
LEFT JOIN FactFlight ff ON ac.AircraftKey = ff.AircraftKey
WHERE ac.IsCurrent = TRUE
GROUP BY ac.Manufacturer, ac.Model, ac.Age
ORDER BY TotalMaintenanceCost DESC;

-- 6. CUSTOMER LOYALTY ANALYSIS
SELECT 
    c.LoyaltyTier,
    COUNT(DISTINCT c.CustomerKey) AS TotalCustomers,
    COUNT(fb.BookingKey) AS TotalBookings,
    SUM(fb.TotalAmount) AS TotalRevenue,
    AVG(fb.TotalAmount) AS AvgRevenuePerBooking,
    SUM(fb.LoyaltyPointsEarned) AS TotalPointsEarned,
    SUM(fb.LoyaltyPointsRedeemed) AS TotalPointsRedeemed,
    ROUND(100.0 * SUM(CASE WHEN fb.CancellationFlag THEN 1 ELSE 0 END) / COUNT(fb.BookingKey), 2) AS CancellationRate
FROM DimCustomer c
JOIN FactBooking fb ON c.CustomerKey = fb.CustomerKey
WHERE c.IsCurrent = TRUE
GROUP BY c.LoyaltyTier
ORDER BY TotalRevenue DESC;

-- 7. ROUTE PROFITABILITY ANALYSIS
SELECT 
    dep.City || ' - ' || arr.City AS Route,
    c.CarrierName,
    COUNT(*) AS TotalFlights,
    SUM(ff.Revenue) AS TotalRevenue,
    SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees) AS TotalCosts,
    SUM(ff.Revenue) - SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees) AS NetProfit,
    ROUND((SUM(ff.Revenue) - SUM(ff.FuelCost + ff.CrewCost + ff.AirportFees)) / NULLIF(SUM(ff.Revenue), 0) * 100, 2) AS ProfitMargin,
    AVG(ff.LoadFactor) AS AvgLoadFactor
FROM FactFlight ff
JOIN DimFlight f ON ff.FlightKey = f.FlightKey
JOIN DimAirport dep ON ff.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON ff.ArrivalAirportKey = arr.AirportKey
JOIN DimCarrier c ON ff.CarrierKey = c.CarrierKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Year = 2024
  AND ff.CancellationFlag = FALSE
GROUP BY Route, c.CarrierName
HAVING COUNT(*) >= 20
ORDER BY NetProfit DESC;

-- 8. DELAY CAUSE BREAKDOWN
SELECT 
    d.Year,
    d.MonthName,
    COUNT(*) AS TotalDelayedFlights,
    SUM(CASE WHEN ff.WeatherImpactFlag THEN 1 ELSE 0 END) AS WeatherDelays,
    SUM(CASE WHEN ff.MechanicalDelayFlag THEN 1 ELSE 0 END) AS MechanicalDelays,
    AVG(ff.ArrivalDelayMin) AS AvgDelayMinutes,
    SUM(ff.Revenue) AS ImpactedRevenue
FROM FactFlight ff
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE ff.ArrivalDelayMin > 15
  AND d.Year = 2024
GROUP BY d.Year, d.MonthName, d.Month
ORDER BY d.Month;

-- 9. CARGO PERFORMANCE METRICS
SELECT 
    dep.City AS Origin,
    arr.City AS Destination,
    d.Year,
    d.Quarter,
    COUNT(*) AS TotalShipments,
    SUM(fc.WeightKg) AS TotalWeightKg,
    SUM(fc.Revenue) AS TotalRevenue,
    AVG(fc.Revenue / NULLIF(fc.WeightKg, 0)) AS RevenuePerKg,
    ROUND(100.0 * SUM(CASE WHEN fc.OnTimeDeliveryFlag THEN 1 ELSE 0 END) / COUNT(*), 2) AS OnTimeDeliveryRate
FROM FactCargo fc
JOIN DimAirport dep ON fc.DepartureAirportKey = dep.AirportKey
JOIN DimAirport arr ON fc.ArrivalAirportKey = arr.AirportKey
JOIN DimDate d ON fc.DateKey = d.DateKey
WHERE d.Year >= 2023
GROUP BY dep.City, arr.City, d.Year, d.Quarter
ORDER BY TotalRevenue DESC;

-- 10. CAPACITY UTILIZATION TRENDS
SELECT 
    d.Year,
    d.MonthName,
    c.CarrierName,
    COUNT(*) AS TotalFlights,
    SUM(ff.SeatsAvailable) AS TotalSeatsAvailable,
    SUM(ff.SeatsFilled) AS TotalSeatsFilled,
    ROUND(100.0 * SUM(ff.SeatsFilled) / NULLIF(SUM(ff.SeatsAvailable), 0), 2) AS OverallLoadFactor,
    SUM(ff.Revenue) AS TotalRevenue
FROM FactFlight ff
JOIN DimCarrier c ON ff.CarrierKey = c.CarrierKey
JOIN DimDate d ON ff.DateKey = d.DateKey
WHERE d.Year = 2024
GROUP BY d.Year, d.MonthName, d.Month, c.CarrierName
ORDER BY d.Month, OverallLoadFactor DESC;
