#!/usr/bin/env python3
"""
Load sample data into Airline Analytics Data Warehouse
"""
import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

fake = Faker()

def get_db_connection():
    """Connect to database using DATABASE_URL from .env"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    return psycopg2.connect(database_url)

def load_airports(conn):
    """Load sample airport data"""
    airports = [
        (1, 'JFK', 'KJFK', 'John F Kennedy Intl', 'New York', 'NY', 'USA', 'North America', 40.6413, -73.7781, 'America/New_York'),
        (2, 'LAX', 'KLAX', 'Los Angeles Intl', 'Los Angeles', 'CA', 'USA', 'North America', 33.9416, -118.4085, 'America/Los_Angeles'),
        (3, 'ORD', 'KORD', "O'Hare Intl", 'Chicago', 'IL', 'USA', 'North America', 41.9742, -87.9073, 'America/Chicago'),
        (4, 'ATL', 'KATL', 'Hartsfield-Jackson Intl', 'Atlanta', 'GA', 'USA', 'North America', 33.6407, -84.4277, 'America/New_York'),
        (5, 'DFW', 'KDFW', 'Dallas Fort Worth Intl', 'Dallas', 'TX', 'USA', 'North America', 32.8998, -97.0403, 'America/Chicago'),
        (6, 'LHR', 'EGLL', 'London Heathrow', 'London', None, 'UK', 'Europe', 51.4700, -0.4543, 'Europe/London'),
        (7, 'CDG', 'LFPG', 'Charles de Gaulle', 'Paris', None, 'France', 'Europe', 49.0097, 2.5479, 'Europe/Paris'),
        (8, 'DXB', 'OMDB', 'Dubai Intl', 'Dubai', None, 'UAE', 'Middle East', 25.2532, 55.3657, 'Asia/Dubai'),
        (9, 'NRT', 'RJAA', 'Narita Intl', 'Tokyo', None, 'Japan', 'Asia', 35.7720, 140.3929, 'Asia/Tokyo'),
        (10, 'SYD', 'YSSY', 'Sydney Kingsford Smith', 'Sydney', 'NSW', 'Australia', 'Oceania', -33.9399, 151.1753, 'Australia/Sydney')
    ]
    
    with conn.cursor() as cur:
        for airport in airports:
            cur.execute("""
                INSERT INTO DimAirport (AirportKey, IATA, ICAO, AirportName, City, State, Country, Region, 
                                       Latitude, Longitude, Timezone, EffectiveDate, IsCurrent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE)
                ON CONFLICT (AirportKey) DO NOTHING
            """, airport)
    conn.commit()
    print("✓ Loaded airports")

def load_carriers(conn):
    """Load sample carrier data"""
    carriers = [
        (1, 'AA', 'American Airlines', True, 'Oneworld', 'USA'),
        (2, 'DL', 'Delta Air Lines', True, 'SkyTeam', 'USA'),
        (3, 'UA', 'United Airlines', True, 'Star Alliance', 'USA'),
        (4, 'BA', 'British Airways', True, 'Oneworld', 'UK'),
        (5, 'EK', 'Emirates', True, None, 'UAE')
    ]
    
    with conn.cursor() as cur:
        for carrier in carriers:
            cur.execute("""
                INSERT INTO DimCarrier (CarrierKey, AirlineCode, CarrierName, OperatingCarrierFlag, 
                                       AllianceCode, Country, EffectiveDate, IsCurrent)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE)
                ON CONFLICT (CarrierKey) DO NOTHING
            """, carrier)
    conn.commit()
    print("✓ Loaded carriers")

def load_aircraft(conn):
    """Load sample aircraft data"""
    aircraft_types = [
        ('Boeing', '737-800', 175, 2000),
        ('Boeing', '777-300ER', 350, 5000),
        ('Airbus', 'A320', 180, 2200),
        ('Airbus', 'A350-900', 325, 4500)
    ]
    
    with conn.cursor() as cur:
        for i in range(1, 21):
            mfr, model, capacity, cargo = random.choice(aircraft_types)
            year = random.randint(2010, 2023)
            age = 2024 - year
            cur.execute("""
                INSERT INTO DimAircraft (AircraftKey, TailNumber, AircraftType, Manufacturer, Model,
                                        SeatingCapacity, CargoCapacityKg, ManufactureYear, Age,
                                        OwnershipType, EffectiveDate, IsCurrent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE)
                ON CONFLICT (AircraftKey) DO NOTHING
            """, (i, f'N{1000+i}AA', f'{mfr} {model}', mfr, model, capacity, cargo, year, age, 'Owned'))
    conn.commit()
    print("✓ Loaded aircraft")

def load_customers(conn):
    """Load sample customer data"""
    tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
    
    with conn.cursor() as cur:
        for i in range(1, 101):
            cur.execute("""
                INSERT INTO DimCustomer (CustomerKey, CustomerID, FirstName, LastName, Email,
                                        LoyaltyTier, LoyaltyPoints, JoinDate, BirthDate, Gender,
                                        Country, City, EffectiveDate, IsCurrent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE)
                ON CONFLICT (CustomerKey) DO NOTHING
            """, (i, f'CUST{10000+i}', fake.first_name(), fake.last_name(), fake.email(),
                  random.choice(tiers), random.randint(0, 50000), fake.date_between(start_date='-5y'),
                  fake.date_of_birth(minimum_age=18, maximum_age=80), random.choice(['M', 'F']),
                  'USA', fake.city()))
    conn.commit()
    print("✓ Loaded customers")

def load_reference_data(conn):
    """Load fare classes, weather, and maintenance types"""
    fare_classes = [
        (1, 'Y', 'Economy Basic', 'Economy', 'Low', False, True, 1, False),
        (2, 'M', 'Economy Standard', 'Economy', 'Medium', True, True, 2, False),
        (3, 'W', 'Premium Economy', 'Premium Economy', 'Medium', True, False, 2, True),
        (4, 'J', 'Business', 'Business', 'High', True, False, 3, True),
        (5, 'F', 'First Class', 'First', 'Premium', True, False, 3, True)
    ]
    
    weather_conditions = [
        (1, 'Clear', 'None', 'Low', 'Clear skies'),
        (2, 'Rain', 'Moderate', 'Medium', 'Moderate rainfall'),
        (3, 'Thunderstorm', 'Severe', 'High', 'Severe thunderstorms'),
        (4, 'Snow', 'Moderate', 'Medium', 'Snowfall'),
        (5, 'Fog', 'Severe', 'High', 'Dense fog')
    ]
    
    maintenance_types = [
        (1, 'A-CHECK', 'Routine', 'Basic inspection', True, 8),
        (2, 'C-CHECK', 'Major', 'Comprehensive inspection', True, 120),
        (3, 'ENGINE', 'Engine', 'Engine maintenance', True, 48),
        (4, 'EMERGENCY', 'Unscheduled', 'Emergency repair', False, 12)
    ]
    
    with conn.cursor() as cur:
        for fc in fare_classes:
            cur.execute("""
                INSERT INTO DimFareClass VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (FareClassKey) DO NOTHING
            """, fc)
        
        for wc in weather_conditions:
            cur.execute("""
                INSERT INTO DimWeather VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (WeatherKey) DO NOTHING
            """, wc)
        
        for mt in maintenance_types:
            cur.execute("""
                INSERT INTO DimMaintenanceType VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (MaintenanceTypeKey) DO NOTHING
            """, mt)
    
    conn.commit()
    print("✓ Loaded reference data")

def main():
    try:
        conn = get_db_connection()
        print("Loading sample data...")
        
        load_airports(conn)
        load_carriers(conn)
        load_aircraft(conn)
        load_customers(conn)
        load_reference_data(conn)
        
        print("\n✓ Sample data loaded successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
