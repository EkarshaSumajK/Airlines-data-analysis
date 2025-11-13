-- =====================================================
-- AIRLINE ANALYTICS DATA WAREHOUSE - FACT TABLES
-- =====================================================

-- Flight Facts (Operational Performance)
CREATE TABLE FactFlight (
    FlightFactKey BIGINT PRIMARY KEY,
    FlightKey INT NOT NULL,
    DateKey INT NOT NULL,
    AircraftKey INT NOT NULL,
    DepartureAirportKey INT NOT NULL,
    ArrivalAirportKey INT NOT NULL,
    CarrierKey INT NOT NULL,
    WeatherKey INT,
    
    -- Timing metrics
    ActualDepartureTime TIMESTAMP,
    ActualArrivalTime TIMESTAMP,
    DepartureDelayMin INT DEFAULT 0,
    ArrivalDelayMin INT DEFAULT 0,
    TaxiOutMin INT,
    TaxiInMin INT,
    AirTimeMin INT,
    
    -- Capacity metrics
    SeatsAvailable INT,
    SeatsFilled INT,
    LoadFactor DECIMAL(5,2),
    
    -- Financial metrics
    Revenue DECIMAL(12,2),
    FuelCost DECIMAL(10,2),
    CrewCost DECIMAL(10,2),
    AirportFees DECIMAL(10,2),
    
    -- Operational flags
    CancellationFlag BOOLEAN DEFAULT FALSE,
    DiversionFlag BOOLEAN DEFAULT FALSE,
    WeatherImpactFlag BOOLEAN DEFAULT FALSE,
    MechanicalDelayFlag BOOLEAN DEFAULT FALSE,
    
    -- Distance
    DistanceMiles INT,
    
    FOREIGN KEY (FlightKey) REFERENCES DimFlight(FlightKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (AircraftKey) REFERENCES DimAircraft(AircraftKey),
    FOREIGN KEY (DepartureAirportKey) REFERENCES DimAirport(AirportKey),
    FOREIGN KEY (ArrivalAirportKey) REFERENCES DimAirport(AirportKey),
    FOREIGN KEY (CarrierKey) REFERENCES DimCarrier(CarrierKey),
    FOREIGN KEY (WeatherKey) REFERENCES DimWeather(WeatherKey)
);

-- Booking Facts (Revenue and Customer)
CREATE TABLE FactBooking (
    BookingKey BIGINT PRIMARY KEY,
    FlightKey INT NOT NULL,
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    FareClassKey INT NOT NULL,
    
    -- Booking details
    BookingDate DATE,
    TravelDate DATE,
    
    -- Financial metrics
    TicketPrice DECIMAL(10,2),
    Taxes DECIMAL(8,2),
    Fees DECIMAL(8,2),
    TotalAmount DECIMAL(10,2),
    RefundAmount DECIMAL(10,2) DEFAULT 0,
    
    -- Booking status
    BookingStatus VARCHAR(20),
    CancellationFlag BOOLEAN DEFAULT FALSE,
    NoShowFlag BOOLEAN DEFAULT FALSE,
    
    -- Loyalty
    LoyaltyPointsEarned INT DEFAULT 0,
    LoyaltyPointsRedeemed INT DEFAULT 0,
    
    FOREIGN KEY (FlightKey) REFERENCES DimFlight(FlightKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    FOREIGN KEY (FareClassKey) REFERENCES DimFareClass(FareClassKey)
);

-- Cargo Facts
CREATE TABLE FactCargo (
    CargoKey BIGINT PRIMARY KEY,
    FlightKey INT NOT NULL,
    DateKey INT NOT NULL,
    DepartureAirportKey INT NOT NULL,
    ArrivalAirportKey INT NOT NULL,
    
    -- Cargo metrics
    WeightKg DECIMAL(10,2),
    VolumeCubicMeters DECIMAL(10,2),
    NumberOfPieces INT,
    CargoType VARCHAR(50),
    
    -- Financial
    Revenue DECIMAL(10,2),
    HandlingCost DECIMAL(8,2),
    
    -- Status
    OnTimeDeliveryFlag BOOLEAN DEFAULT TRUE,
    DamageFlag BOOLEAN DEFAULT FALSE,
    
    FOREIGN KEY (FlightKey) REFERENCES DimFlight(FlightKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (DepartureAirportKey) REFERENCES DimAirport(AirportKey),
    FOREIGN KEY (ArrivalAirportKey) REFERENCES DimAirport(AirportKey)
);

-- Maintenance Facts
CREATE TABLE FactMaintenance (
    MaintenanceKey BIGINT PRIMARY KEY,
    AircraftKey INT NOT NULL,
    DateKey INT NOT NULL,
    MaintenanceTypeKey INT NOT NULL,
    
    -- Maintenance details
    MaintenanceDate DATE,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    DurationHours DECIMAL(6,2),
    
    -- Financial
    LaborCost DECIMAL(10,2),
    PartsCost DECIMAL(10,2),
    TotalCost DECIMAL(10,2),
    
    -- Impact
    FlightsAffected INT DEFAULT 0,
    DowntimeHours DECIMAL(6,2),
    
    -- Status
    CompletionStatus VARCHAR(20),
    UnscheduledFlag BOOLEAN DEFAULT FALSE,
    
    FOREIGN KEY (AircraftKey) REFERENCES DimAircraft(AircraftKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (MaintenanceTypeKey) REFERENCES DimMaintenanceType(MaintenanceTypeKey)
);

-- Create indexes for query performance
CREATE INDEX idx_factflight_date ON FactFlight(DateKey);
CREATE INDEX idx_factflight_flight ON FactFlight(FlightKey);
CREATE INDEX idx_factflight_aircraft ON FactFlight(AircraftKey);
CREATE INDEX idx_factflight_route ON FactFlight(DepartureAirportKey, ArrivalAirportKey);

CREATE INDEX idx_factbooking_date ON FactBooking(DateKey);
CREATE INDEX idx_factbooking_customer ON FactBooking(CustomerKey);
CREATE INDEX idx_factbooking_flight ON FactBooking(FlightKey);

CREATE INDEX idx_factcargo_date ON FactCargo(DateKey);
CREATE INDEX idx_factcargo_flight ON FactCargo(FlightKey);

CREATE INDEX idx_factmaintenance_date ON FactMaintenance(DateKey);
CREATE INDEX idx_factmaintenance_aircraft ON FactMaintenance(AircraftKey);

-- Partitioning strategy (example for PostgreSQL)
-- ALTER TABLE FactFlight PARTITION BY RANGE (DateKey);
-- ALTER TABLE FactBooking PARTITION BY RANGE (DateKey);
