-- =====================================================
-- AIRLINE ANALYTICS DATA WAREHOUSE - DIMENSION TABLES
-- =====================================================

-- Date Dimension
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE NOT NULL,
    DayOfWeek VARCHAR(10),
    DayOfMonth INT,
    Month INT,
    MonthName VARCHAR(10),
    Quarter INT,
    Year INT,
    HolidayFlag BOOLEAN DEFAULT FALSE,
    IsWeekend BOOLEAN DEFAULT FALSE,
    FiscalYear INT,
    FiscalQuarter INT
);

-- Airport Dimension
CREATE TABLE DimAirport (
    AirportKey INT PRIMARY KEY,
    IATA VARCHAR(3) NOT NULL,
    ICAO VARCHAR(4),
    AirportName VARCHAR(100),
    City VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50),
    Region VARCHAR(50),
    Latitude DECIMAL(9,6),
    Longitude DECIMAL(9,6),
    Timezone VARCHAR(50),
    EffectiveDate DATE,
    ExpirationDate DATE,
    IsCurrent BOOLEAN DEFAULT TRUE
);

-- Carrier/Route Dimension
CREATE TABLE DimCarrier (
    CarrierKey INT PRIMARY KEY,
    AirlineCode VARCHAR(10) NOT NULL,
    CarrierName VARCHAR(100),
    OperatingCarrierFlag BOOLEAN DEFAULT TRUE,
    AllianceCode VARCHAR(20),
    Country VARCHAR(50),
    EffectiveDate DATE,
    ExpirationDate DATE,
    IsCurrent BOOLEAN DEFAULT TRUE
);

-- Aircraft Dimension (SCD Type 2)
CREATE TABLE DimAircraft (
    AircraftKey INT PRIMARY KEY,
    TailNumber VARCHAR(20) NOT NULL,
    AircraftType VARCHAR(50),
    Manufacturer VARCHAR(50),
    Model VARCHAR(50),
    SeatingCapacity INT,
    CargoCapacityKg INT,
    ManufactureYear INT,
    Age INT,
    OwnershipType VARCHAR(20),
    MaintenanceCycle VARCHAR(20),
    EffectiveDate DATE,
    ExpirationDate DATE,
    IsCurrent BOOLEAN DEFAULT TRUE
);

-- Customer Dimension (SCD Type 2)
CREATE TABLE DimCustomer (
    CustomerKey INT PRIMARY KEY,
    CustomerID VARCHAR(50) NOT NULL,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    Phone VARCHAR(20),
    LoyaltyTier VARCHAR(20),
    LoyaltyPoints INT,
    JoinDate DATE,
    BirthDate DATE,
    Gender VARCHAR(10),
    Country VARCHAR(50),
    City VARCHAR(50),
    EffectiveDate DATE,
    ExpirationDate DATE,
    IsCurrent BOOLEAN DEFAULT TRUE
);

-- Fare Class Dimension
CREATE TABLE DimFareClass (
    FareClassKey INT PRIMARY KEY,
    FareCode VARCHAR(10) NOT NULL,
    ClassName VARCHAR(50),
    CabinClass VARCHAR(20),
    PriceBand VARCHAR(20),
    RefundableFlag BOOLEAN DEFAULT FALSE,
    ChangeFeeFlag BOOLEAN DEFAULT TRUE,
    BaggageAllowance INT,
    PriorityBoarding BOOLEAN DEFAULT FALSE
);

-- Flight Dimension
CREATE TABLE DimFlight (
    FlightKey INT PRIMARY KEY,
    FlightNumber VARCHAR(20) NOT NULL,
    CarrierKey INT,
    DepartureAirportKey INT,
    ArrivalAirportKey INT,
    ScheduledDepartureTime TIME,
    ScheduledArrivalTime TIME,
    ScheduledDurationMin INT,
    DistanceMiles INT,
    RouteCode VARCHAR(20),
    FOREIGN KEY (CarrierKey) REFERENCES DimCarrier(CarrierKey),
    FOREIGN KEY (DepartureAirportKey) REFERENCES DimAirport(AirportKey),
    FOREIGN KEY (ArrivalAirportKey) REFERENCES DimAirport(AirportKey)
);

-- Weather Dimension
CREATE TABLE DimWeather (
    WeatherKey INT PRIMARY KEY,
    WeatherCondition VARCHAR(50),
    Severity VARCHAR(20),
    ImpactLevel VARCHAR(20),
    Description VARCHAR(200)
);

-- Maintenance Type Dimension
CREATE TABLE DimMaintenanceType (
    MaintenanceTypeKey INT PRIMARY KEY,
    MaintenanceCode VARCHAR(20),
    MaintenanceCategory VARCHAR(50),
    Description VARCHAR(200),
    IsScheduled BOOLEAN DEFAULT TRUE,
    AverageDurationHours DECIMAL(5,2)
);

-- Create indexes for performance
CREATE INDEX idx_dimdate_date ON DimDate(Date);
CREATE INDEX idx_dimairport_iata ON DimAirport(IATA);
CREATE INDEX idx_dimcarrier_code ON DimCarrier(AirlineCode);
CREATE INDEX idx_dimaircraft_tail ON DimAircraft(TailNumber, IsCurrent);
CREATE INDEX idx_dimcustomer_id ON DimCustomer(CustomerID, IsCurrent);
CREATE INDEX idx_dimflight_number ON DimFlight(FlightNumber);
