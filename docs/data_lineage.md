# Data Lineage Documentation

## Overview
This document traces data flow from source systems through staging, transformation, and into the data warehouse.

## Source to Target Mapping

### Flight Operations Data

**Source System**: Flight Operations Database (OLTP)
**Extract Frequency**: Hourly incremental, Daily full refresh

| Source Table | Source Column | Target Table | Target Column | Transformation |
|--------------|---------------|--------------|---------------|----------------|
| flights | flight_id | DimFlight | FlightKey | Surrogate key lookup |
| flights | flight_number | DimFlight | FlightNumber | Direct mapping |
| flights | departure_airport | DimAirport | IATA | Lookup dimension |
| flights | arrival_airport | DimAirport | IATA | Lookup dimension |
| flight_events | actual_departure | FactFlight | ActualDepartureTime | Direct mapping |
| flight_events | actual_arrival | FactFlight | ActualArrivalTime | Direct mapping |
| flight_events | scheduled_departure | - | - | Used to calculate DepartureDelayMin |
| flight_events | scheduled_arrival | - | - | Used to calculate ArrivalDelayMin |
| flight_events | seats_sold | FactFlight | SeatsFilled | Direct mapping |
| aircraft | tail_number | DimAircraft | TailNumber | SCD Type 2 lookup |

**Derived Metrics**:
- `DepartureDelayMin` = ActualDepartureTime - ScheduledDepartureTime (in minutes)
- `ArrivalDelayMin` = ActualArrivalTime - ScheduledArrivalTime (in minutes)
- `LoadFactor` = (SeatsFilled / SeatsAvailable) × 100

### Booking/Reservation Data

**Source System**: Reservation System (OLTP)
**Extract Frequency**: Real-time CDC or 15-minute incremental

| Source Table | Source Column | Target Table | Target Column | Transformation |
|--------------|---------------|--------------|---------------|----------------|
| bookings | booking_id | FactBooking | BookingKey | Direct mapping |
| bookings | customer_id | DimCustomer | CustomerID | SCD Type 2 lookup |
| bookings | flight_id | DimFlight | FlightKey | Dimension lookup |
| bookings | fare_code | DimFareClass | FareCode | Dimension lookup |
| bookings | base_fare | FactBooking | TicketPrice | Direct mapping |
| bookings | taxes | FactBooking | Taxes | Direct mapping |
| bookings | fees | FactBooking | Fees | Direct mapping |
| bookings | booking_status | FactBooking | BookingStatus | Direct mapping |
| loyalty_transactions | points_earned | FactBooking | LoyaltyPointsEarned | Aggregated by booking |

**Derived Metrics**:
- `TotalAmount` = TicketPrice + Taxes + Fees
- `CancellationFlag` = BookingStatus IN ('CANCELLED', 'REFUNDED')

### Maintenance Data

**Source System**: Maintenance Management System
**Extract Frequency**: Daily batch

| Source Table | Source Column | Target Table | Target Column | Transformation |
|--------------|---------------|--------------|---------------|----------------|
| maintenance_events | event_id | FactMaintenance | MaintenanceKey | Direct mapping |
| maintenance_events | tail_number | DimAircraft | TailNumber | Dimension lookup |
| maintenance_events | maintenance_type | DimMaintenanceType | MaintenanceCode | Dimension lookup |
| maintenance_events | start_time | FactMaintenance | StartTime | Direct mapping |
| maintenance_events | end_time | FactMaintenance | EndTime | Direct mapping |
| maintenance_costs | labor_cost | FactMaintenance | LaborCost | Aggregated by event |
| maintenance_costs | parts_cost | FactMaintenance | PartsCost | Aggregated by event |

**Derived Metrics**:
- `DurationHours` = (EndTime - StartTime) in hours
- `TotalCost` = LaborCost + PartsCost
- `DowntimeHours` = Time aircraft unavailable for service

### Customer Data

**Source System**: Customer Relationship Management (CRM)
**Extract Frequency**: Daily batch with SCD Type 2 processing

| Source Table | Source Column | Target Table | Target Column | Transformation |
|--------------|---------------|--------------|---------------|----------------|
| customers | customer_id | DimCustomer | CustomerID | Business key |
| customers | first_name | DimCustomer | FirstName | Direct mapping |
| customers | last_name | DimCustomer | LastName | Direct mapping |
| customers | email | DimCustomer | Email | Direct mapping |
| loyalty_members | tier | DimCustomer | LoyaltyTier | Direct mapping |
| loyalty_members | points_balance | DimCustomer | LoyaltyPoints | Direct mapping |
| loyalty_members | join_date | DimCustomer | JoinDate | Direct mapping |

**SCD Type 2 Logic**:
- Track changes to: LoyaltyTier, Email, Phone
- Create new record with new EffectiveDate when changes detected
- Expire old record by setting IsCurrent = FALSE and ExpirationDate

### Weather Data

**Source System**: Weather API (External)
**Extract Frequency**: Hourly

| Source Field | Target Table | Target Column | Transformation |
|--------------|--------------|---------------|----------------|
| condition | DimWeather | WeatherCondition | Standardized values |
| severity | DimWeather | Severity | Mapped to Low/Medium/High |
| airport_code | - | - | Used for flight matching |
| timestamp | - | - | Used for flight matching |

## Data Quality Rules

### Validation Rules Applied During ETL

1. **Referential Integrity**
   - All foreign keys must exist in dimension tables
   - Orphaned records logged and rejected

2. **Range Checks**
   - LoadFactor: 0 ≤ value ≤ 100
   - Revenue: value ≥ 0
   - DelayMinutes: -60 ≤ value ≤ 1440 (allow early arrivals, max 24hr delay)

3. **Null Handling**
   - Required fields: FlightKey, DateKey, AircraftKey, AirportKeys
   - Optional fields: WeatherKey, specific cost components
   - Default values: DelayMin = 0, CancellationFlag = FALSE

4. **Duplicate Detection**
   - Check for duplicate FlightFactKey before insert
   - Use UPSERT logic for late-arriving updates

5. **Date Validation**
   - Flight dates must exist in DimDate
   - No future dates in historical facts
   - Arrival date/time must be after departure

## Data Flow Diagram

```
┌─────────────────┐
│ Source Systems  │
│  - Flight Ops   │
│  - Reservations │
│  - Maintenance  │
│  - CRM          │
│  - Weather API  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Staging Area   │
│  - Raw extracts │
│  - Minimal xform│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ ETL Processing  │
│  - Validation   │
│  - Transform    │
│  - SCD Logic    │
│  - Enrichment   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Warehouse │
│  - Dimensions   │
│  - Facts        │
│  - Aggregates   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   BI Layer      │
│  - Dashboards   │
│  - Reports      │
│  - Ad-hoc Query │
└─────────────────┘
```

## Transformation Logic

### Load Factor Calculation
```python
load_factor = (seats_filled / seats_available) * 100
if load_factor < 0 or load_factor > 100:
    log_error("Invalid load factor")
    reject_record()
```

### RASM Calculation
```sql
RASM = Revenue / (SeatsAvailable * DistanceMiles)
```

### On-Time Performance
```sql
OnTimeFlag = CASE 
    WHEN ArrivalDelayMin <= 15 THEN TRUE 
    ELSE FALSE 
END
```

### SCD Type 2 Customer Update
```python
if customer_exists and attributes_changed:
    # Expire old record
    UPDATE DimCustomer 
    SET IsCurrent = FALSE, ExpirationDate = CURRENT_DATE
    WHERE CustomerKey = existing_key
    
    # Insert new record
    INSERT INTO DimCustomer (CustomerKey, ..., EffectiveDate, IsCurrent)
    VALUES (new_key, ..., CURRENT_DATE, TRUE)
else:
    # No change, skip
    pass
```

## Data Refresh Schedule

| Data Source | Frequency | Method | Latency |
|-------------|-----------|--------|---------|
| Flight Operations | Hourly | Incremental (CDC) | 1 hour |
| Reservations | 15 minutes | Incremental (CDC) | 15 min |
| Maintenance | Daily | Full refresh | 1 day |
| Customer | Daily | SCD Type 2 | 1 day |
| Weather | Hourly | API pull | 1 hour |
| Cargo | Daily | Incremental | 1 day |

## Audit Trail

All ETL processes log:
- Start/end timestamps
- Records processed/inserted/updated/rejected
- Error messages and rejected records
- Data quality check results
- Source system watermarks (last processed timestamp/ID)

Audit tables:
- `etl_audit_log`: High-level job execution
- `etl_error_log`: Detailed error records
- `data_quality_log`: DQ check results
