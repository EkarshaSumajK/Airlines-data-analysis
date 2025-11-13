#!/bin/bash
# Airline Analytics - Automated Setup Script

set -e  # Exit on error

echo "=========================================="
echo "Airline Analytics - Automated Setup"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if PostgreSQL is running
echo "Checking PostgreSQL..."
if ! pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
    echo -e "${RED}✗ PostgreSQL is not running${NC}"
    echo "Please start PostgreSQL first:"
    echo "  brew services start postgresql@14"
    echo "  OR"
    echo "  docker run --name airline-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:14"
    exit 1
fi
echo -e "${GREEN}✓ PostgreSQL is running${NC}"
echo ""

# Check if database exists, create if not
echo "Checking database..."
if psql -lqt | cut -d \| -f 1 | grep -qw airline_analytics; then
    echo -e "${YELLOW}! Database 'airline_analytics' already exists${NC}"
    read -p "Do you want to drop and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Dropping existing database..."
        dropdb airline_analytics 2>/dev/null || true
        createdb airline_analytics
        echo -e "${GREEN}✓ Database recreated${NC}"
    else
        echo "Using existing database"
    fi
else
    echo "Creating database..."
    createdb airline_analytics
    echo -e "${GREEN}✓ Database created${NC}"
fi
echo ""

# Check Python and virtual environment
echo "Checking Python environment..."
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
fi

# Activate virtual environment
source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"
echo ""

# Install dependencies
echo "Installing Python dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# Create .env if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cp .env.example .env
    echo -e "${GREEN}✓ .env file created${NC}"
else
    echo -e "${YELLOW}! .env file already exists${NC}"
fi
echo ""

# Setup database schema
echo "Setting up database schema..."
python etl/setup_schema.py
echo ""

# Load sample data
echo "Loading sample data..."
python etl/load_sample_data.py
echo ""

# Generate sample flight data
echo "Generating sample flight data..."
python -c "
import psycopg2
import random
from datetime import datetime, timedelta
import os

conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'localhost'),
    port=os.getenv('DB_PORT', '5432'),
    database=os.getenv('DB_NAME', 'airline_analytics'),
    user=os.getenv('DB_USER', 'postgres'),
    password=os.getenv('DB_PASSWORD', 'postgres')
)

with conn.cursor() as cur:
    # Create flights
    for i in range(1, 51):
        cur.execute('''
            INSERT INTO DimFlight (FlightKey, FlightNumber, CarrierKey, 
                                  DepartureAirportKey, ArrivalAirportKey, DistanceMiles)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (FlightKey) DO NOTHING
        ''', (i, f'FL{1000+i}', random.randint(1,5), random.randint(1,10), 
              random.randint(1,10), random.randint(200, 3000)))
    
    # Generate flight facts
    base_date = datetime.now() - timedelta(days=30)
    fact_key = 1
    
    for day in range(30):
        date = base_date + timedelta(days=day)
        date_key = int(date.strftime('%Y%m%d'))
        
        for flight in range(1, 21):
            seats_available = random.randint(150, 350)
            seats_filled = random.randint(int(seats_available * 0.5), seats_available)
            load_factor = (seats_filled / seats_available) * 100
            
            cur.execute('''
                INSERT INTO FactFlight (
                    FlightFactKey, FlightKey, DateKey, AircraftKey,
                    DepartureAirportKey, ArrivalAirportKey, CarrierKey,
                    DepartureDelayMin, ArrivalDelayMin, SeatsAvailable,
                    SeatsFilled, LoadFactor, Revenue, FuelCost, DistanceMiles,
                    CancellationFlag
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (FlightFactKey) DO NOTHING
            ''', (
                fact_key, random.randint(1, 50), date_key, random.randint(1, 20),
                random.randint(1, 10), random.randint(1, 10), random.randint(1, 5),
                random.randint(-5, 60), random.randint(-5, 90), seats_available,
                seats_filled, round(load_factor, 2), round(seats_filled * random.uniform(100, 500), 2),
                round(random.uniform(5000, 15000), 2), random.randint(200, 3000),
                random.random() < 0.02
            ))
            fact_key += 1

conn.commit()
conn.close()
print('✓ Generated 600 sample flights (30 days × 20 flights/day)')
"
echo ""

# Verify installation
echo "Verifying installation..."
psql -d airline_analytics -c "
SELECT 
    'Airports' AS Dimension, COUNT(*) AS Count FROM DimAirport
UNION ALL
SELECT 'Carriers', COUNT(*) FROM DimCarrier
UNION ALL
SELECT 'Aircraft', COUNT(*) FROM DimAircraft
UNION ALL
SELECT 'Customers', COUNT(*) FROM DimCustomer
UNION ALL
SELECT 'Flights', COUNT(*) FROM FactFlight
ORDER BY Dimension;
"
echo ""

# Show sample analytics
echo "Sample Analytics - On-Time Performance:"
psql -d airline_analytics -c "
SELECT 
    COUNT(*) AS TotalFlights,
    ROUND(AVG(LoadFactor), 2) AS AvgLoadFactor,
    ROUND(100.0 * SUM(CASE WHEN ArrivalDelayMin <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS OnTimePercentage,
    ROUND(AVG(ArrivalDelayMin), 2) AS AvgDelayMinutes
FROM FactFlight
WHERE CancellationFlag = FALSE;
"
echo ""

echo -e "${GREEN}=========================================="
echo "✓ Setup Complete!"
echo "==========================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Run analytics queries:"
echo "     psql -d airline_analytics -f sql/04_analytics_queries.sql"
echo ""
echo "  2. Start Jupyter notebook:"
echo "     jupyter notebook notebooks/airline_analytics.ipynb"
echo ""
echo "  3. Create dashboard views:"
echo "     psql -d airline_analytics -f analytics/dashboard_queries.sql"
echo ""
echo "  4. Connect to database:"
echo "     psql -d airline_analytics"
echo ""
echo "Documentation: docs/user_guide.md"
