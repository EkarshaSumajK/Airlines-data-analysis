# ✈️ Airline Analytics Data Warehouse

A comprehensive SQL-based data warehouse project for airline operations analytics, built with PostgreSQL and deployed on Neon serverless database.

## 📋 Project Overview

This project demonstrates enterprise-level data warehouse design and implementation for airline analytics, featuring:

- **Star Schema Design** with dimension and fact tables
- **Slowly Changing Dimensions (SCD Type 2)** for historical tracking
- **ETL Pipeline** with data quality checks
- **Advanced SQL Analytics** for business intelligence
- **Interactive Dashboard** for data visualization

## 🏗️ Architecture

### Data Warehouse Schema

**Dimension Tables:**
- `DimDate` - Date dimension with fiscal calendar
- `DimAirport` - Airport master data with geolocation
- `DimCarrier` - Airline carriers and alliances
- `DimAircraft` - Fleet information (SCD Type 2)
- `DimCustomer` - Customer profiles with loyalty tiers (SCD Type 2)
- `DimFareClass` - Fare class definitions
- `DimFlight` - Flight schedule master
- `DimWeather` - Weather conditions
- `DimMaintenanceType` - Maintenance categories

**Fact Tables:**
- `FactFlight` - Flight operational metrics (delays, capacity, revenue)
- `FactBooking` - Booking and revenue details
- `FactCargo` - Cargo operations
- `FactMaintenance` - Aircraft maintenance records

## 🚀 Tech Stack

- **Database:** PostgreSQL (Neon Serverless)
- **ETL:** Python with psycopg2
- **Visualization:** Streamlit + Plotly
- **Data Generation:** Faker library

## 📁 Project Structure

```
sql/
├── sql/
│   ├── 01_create_dimensions.sql    # Dimension table DDL
│   ├── 02_create_facts.sql         # Fact table DDL
│   ├── 03_populate_date_dimension.sql  # Date dimension data
│   └── 04_analytics_queries.sql    # Business intelligence queries
├── etl/
│   ├── etl_pipeline.py             # ETL orchestration
│   └── load_sample_data.py         # Sample data loader
├── docs/
│   ├── architecture.md             # Architecture documentation
│   ├── data_dictionary.md          # Data dictionary
│   └── user_guide.md               # User guide
├── app.py                          # Streamlit dashboard
├── setup_database.py               # Database initialization
└── requirements.txt                # Python dependencies
```

## 🛠️ Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd sql
```

### 2. Set Up Environment

Create a `.env` file with your Neon database credentials:

```bash
DATABASE_URL='postgresql://user:password@endpoint.neon.tech/dbname?sslmode=require'
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize Database

```bash
# Create schema and populate date dimension
python setup_database.py

# Load sample data
python etl/load_sample_data.py
```

### 5. Run the Dashboard

```bash
streamlit run app.py
```

## 📊 Key Features

### 1. Star Schema Design
- Optimized for analytical queries
- Denormalized for query performance
- Proper indexing strategy

### 2. Slowly Changing Dimensions (SCD Type 2)
- Historical tracking of customer changes
- Aircraft lifecycle management
- Effective/expiration date tracking

### 3. ETL Pipeline
- Incremental data loading
- Data quality validation
- Error handling and logging
- Upsert logic for fact tables

### 4. Advanced Analytics
- Flight performance metrics
- Revenue analysis
- Customer segmentation
- Operational KPIs

## 🔍 Sample Queries

### Top Routes by Revenue
```sql
SELECT 
    da.City || ' → ' || aa.City as Route,
    SUM(ff.Revenue) as TotalRevenue,
    COUNT(*) as FlightCount
FROM FactFlight ff
JOIN DimAirport da ON ff.DepartureAirportKey = da.AirportKey
JOIN DimAirport aa ON ff.ArrivalAirportKey = aa.AirportKey
GROUP BY Route
ORDER BY TotalRevenue DESC
LIMIT 10;
```

### Customer Loyalty Analysis
```sql
SELECT 
    LoyaltyTier,
    COUNT(*) as CustomerCount,
    AVG(LoyaltyPoints) as AvgPoints
FROM DimCustomer
WHERE IsCurrent = TRUE
GROUP BY LoyaltyTier
ORDER BY AvgPoints DESC;
```

### Fleet Utilization
```sql
SELECT 
    Manufacturer,
    Model,
    COUNT(*) as AircraftCount,
    AVG(Age) as AvgAge
FROM DimAircraft
WHERE IsCurrent = TRUE
GROUP BY Manufacturer, Model
ORDER BY AircraftCount DESC;
```

## 📈 Dashboard Features

The Streamlit dashboard provides:

- **Real-time Metrics:** Airports, carriers, fleet, customers
- **Interactive Maps:** Global airport network visualization
- **Fleet Analytics:** Manufacturer distribution, age analysis
- **Customer Insights:** Loyalty tiers, top customers
- **Data Quality:** Dimension coverage and statistics

## 🎯 Learning Outcomes

This project demonstrates:

1. **Data Warehouse Design**
   - Star schema modeling
   - Dimension vs fact table design
   - Surrogate key management

2. **Advanced SQL Concepts**
   - Window functions
   - CTEs (Common Table Expressions)
   - Recursive queries
   - Complex joins

3. **ETL Development**
   - Data extraction and transformation
   - Incremental loading strategies
   - Data quality checks
   - SCD Type 2 implementation

4. **Database Optimization**
   - Index strategy
   - Query performance tuning
   - Partitioning considerations

5. **Cloud Database**
   - Serverless PostgreSQL (Neon)
   - Connection pooling
   - Environment management

## 📚 Documentation

- [Architecture Guide](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [User Guide](docs/user_guide.md)

## 🔐 Security

- Database credentials stored in `.env` (gitignored)
- Connection string encryption
- No hardcoded passwords
- Environment variable management

## 🚦 Future Enhancements

- [ ] Real-time flight tracking integration
- [ ] Machine learning for delay prediction
- [ ] Advanced partitioning strategy
- [ ] Materialized views for performance
- [ ] Data quality monitoring dashboard
- [ ] Automated testing suite

## 📝 License

This project is for educational and portfolio purposes.

## 👤 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [Your Profile](https://linkedin.com/in/yourprofile)

## 🙏 Acknowledgments

- Neon for serverless PostgreSQL hosting
- Streamlit for dashboard framework
- Faker for sample data generation

---

**⭐ If you found this project helpful, please give it a star!**
