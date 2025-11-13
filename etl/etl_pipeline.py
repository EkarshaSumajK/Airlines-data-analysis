#!/usr/bin/env python3
"""
ETL Pipeline for Airline Analytics Data Warehouse
Handles incremental loads, SCD Type 2, and data quality checks
"""
import psycopg2
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirlineETL:
    def __init__(self, conn):
        self.conn = conn
    
    def extract_flight_data(self, start_date, end_date):
        """Extract flight data from source systems"""
        logger.info(f"Extracting flight data from {start_date} to {end_date}")
        # Placeholder for actual extraction logic
        return []
    
    def transform_flight_data(self, raw_data):
        """Transform and enrich flight data"""
        logger.info("Transforming flight data")
        transformed = []
        
        for record in raw_data:
            # Calculate derived metrics
            record['LoadFactor'] = (record['SeatsFilled'] / record['SeatsAvailable']) * 100
            record['OnTimeFlag'] = record['ArrivalDelayMin'] <= 15
            
            # Data quality checks
            if not self.validate_flight_record(record):
                logger.warning(f"Invalid record: {record}")
                continue
            
            transformed.append(record)
        
        return transformed
    
    def validate_flight_record(self, record):
        """Data quality validation"""
        required_fields = ['FlightKey', 'DateKey', 'AircraftKey', 'DepartureAirportKey', 'ArrivalAirportKey']
        
        for field in required_fields:
            if field not in record or record[field] is None:
                return False
        
        if record.get('SeatsAvailable', 0) < record.get('SeatsFilled', 0):
            return False
        
        return True
    
    def load_flight_facts(self, data):
        """Load flight facts with upsert logic"""
        logger.info(f"Loading {len(data)} flight records")
        
        with self.conn.cursor() as cur:
            for record in data:
                cur.execute("""
                    INSERT INTO FactFlight (
                        FlightFactKey, FlightKey, DateKey, AircraftKey, DepartureAirportKey,
                        ArrivalAirportKey, CarrierKey, DepartureDelayMin, ArrivalDelayMin,
                        SeatsAvailable, SeatsFilled, LoadFactor, Revenue, FuelCost,
                        DistanceMiles, CancellationFlag
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (FlightFactKey) DO UPDATE SET
                        DepartureDelayMin = EXCLUDED.DepartureDelayMin,
                        ArrivalDelayMin = EXCLUDED.ArrivalDelayMin,
                        SeatsFilled = EXCLUDED.SeatsFilled,
                        LoadFactor = EXCLUDED.LoadFactor
                """, (
                    record['FlightFactKey'], record['FlightKey'], record['DateKey'],
                    record['AircraftKey'], record['DepartureAirportKey'], record['ArrivalAirportKey'],
                    record['CarrierKey'], record['DepartureDelayMin'], record['ArrivalDelayMin'],
                    record['SeatsAvailable'], record['SeatsFilled'], record['LoadFactor'],
                    record['Revenue'], record['FuelCost'], record['DistanceMiles'],
                    record['CancellationFlag']
                ))
        
        self.conn.commit()
        logger.info("Flight facts loaded successfully")
    
    def handle_scd_type2_customer(self, customer_data):
        """Handle SCD Type 2 for customer dimension"""
        logger.info("Processing customer SCD Type 2")
        
        with self.conn.cursor() as cur:
            for customer in customer_data:
                # Check if customer exists and has changed
                cur.execute("""
                    SELECT CustomerKey, LoyaltyTier, Email
                    FROM DimCustomer
                    WHERE CustomerID = %s AND IsCurrent = TRUE
                """, (customer['CustomerID'],))
                
                existing = cur.fetchone()
                
                if existing:
                    # Check if attributes changed
                    if (existing[1] != customer['LoyaltyTier'] or 
                        existing[2] != customer['Email']):
                        
                        # Expire old record
                        cur.execute("""
                            UPDATE DimCustomer
                            SET IsCurrent = FALSE, ExpirationDate = CURRENT_DATE
                            WHERE CustomerKey = %s
                        """, (existing[0],))
                        
                        # Insert new record
                        cur.execute("""
                            INSERT INTO DimCustomer (
                                CustomerKey, CustomerID, FirstName, LastName, Email,
                                LoyaltyTier, LoyaltyPoints, EffectiveDate, IsCurrent
                            ) VALUES (
                                (SELECT COALESCE(MAX(CustomerKey), 0) + 1 FROM DimCustomer),
                                %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE
                            )
                        """, (
                            customer['CustomerID'], customer['FirstName'], customer['LastName'],
                            customer['Email'], customer['LoyaltyTier'], customer['LoyaltyPoints']
                        ))
                else:
                    # New customer
                    cur.execute("""
                        INSERT INTO DimCustomer (
                            CustomerKey, CustomerID, FirstName, LastName, Email,
                            LoyaltyTier, LoyaltyPoints, EffectiveDate, IsCurrent
                        ) VALUES (
                            (SELECT COALESCE(MAX(CustomerKey), 0) + 1 FROM DimCustomer),
                            %s, %s, %s, %s, %s, %s, CURRENT_DATE, TRUE
                        )
                    """, (
                        customer['CustomerID'], customer['FirstName'], customer['LastName'],
                        customer['Email'], customer['LoyaltyTier'], customer['LoyaltyPoints']
                    ))
        
        self.conn.commit()
        logger.info("Customer SCD Type 2 processing completed")
    
    def run_data_quality_checks(self):
        """Run comprehensive data quality checks"""
        logger.info("Running data quality checks")
        
        checks = {
            'orphaned_flights': """
                SELECT COUNT(*) FROM FactFlight ff
                LEFT JOIN DimFlight f ON ff.FlightKey = f.FlightKey
                WHERE f.FlightKey IS NULL
            """,
            'invalid_load_factors': """
                SELECT COUNT(*) FROM FactFlight
                WHERE LoadFactor < 0 OR LoadFactor > 100
            """,
            'future_dates': """
                SELECT COUNT(*) FROM FactFlight ff
                JOIN DimDate d ON ff.DateKey = d.DateKey
                WHERE d.Date > CURRENT_DATE
            """
        }
        
        with self.conn.cursor() as cur:
            for check_name, query in checks.items():
                cur.execute(query)
                count = cur.fetchone()[0]
                if count > 0:
                    logger.warning(f"Data quality issue: {check_name} - {count} records")
                else:
                    logger.info(f"✓ {check_name} passed")

def main():
    # Connect using DATABASE_URL from .env
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables")
    
    conn = psycopg2.connect(database_url)
    
    etl = AirlineETL(conn)
    
    # Run ETL pipeline
    start_date = datetime.now() - timedelta(days=7)
    end_date = datetime.now()
    
    raw_data = etl.extract_flight_data(start_date, end_date)
    transformed_data = etl.transform_flight_data(raw_data)
    etl.load_flight_facts(transformed_data)
    etl.run_data_quality_checks()
    
    conn.close()
    logger.info("ETL pipeline completed")

if __name__ == '__main__':
    main()
