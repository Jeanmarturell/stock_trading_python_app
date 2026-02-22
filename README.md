# Stock Trading Data Pipeline

A robust Python-based ETL pipeline that fetches real-time stock ticker data from the Massive API and ingests it into Snowflake Data Warehouse for analytics and reporting.

## 📋 Table of Contents

- [Overview](#Overview)
- [Architecture](#Architecture)
- [Prerequisites](#Prerequisites)
- [Installation](#Installation)
- [Configuration](#Configuration)
- [Usage](#Usage)
- [Project Structure](#Project-Structure)
- [Error Handling & Rate Limiting](#Error-Handling--Rate-Limiting)
- [Monitoring & Scheduling](#Monitoring--Scheduling)
- [License](#License)
- [Contributing](#Contributing)

## 🎯 Overview

This project automates the collection of stock market ticker data from Massive's comprehensive market data API and orchestrates batch ingestion into Snowflake. It implements production-grade practices including:

- **Rate-limit aware pagination** - Respects API throttling with configurable delays
- **Batch processing** - Optimized insertion in configurable batch sizes (default: 1000 records)
- **Error recovery** - Graceful handling of API failures with retry logic
- **Data partitioning** - Daily snapshot partitioning via `ds` (date snapshot) column
- **Scheduled execution** - Optional cron/Airflow integration for automated runs

## 🏗️ Architecture

```
┌──────────────────┐
│   Massive API    │
│  (Stock Data)    │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────┐
│  script.py                   │
│  - Fetch tickers (paginated) │
│  - Transform & map fields    │
│  - Batch insert to cloud     │
└────────┬─────────────────────┘
         │
         ▼
┌──────────────────┐
│   Snowflake      │
│(Data Warehouse)  │
└──────────────────┘
```

## 📦 Prerequisites

- **Python 3.8+**
- **Massive API Key** - Sign up at [massive.com](https://massive.com)
- **Snowflake Account** - With appropriate warehouse and database privileges
- **Network Access** - Connectivity to both Massive API and Snowflake

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Jeanmarturell/stock_trading_python_app
cd stock_trading_python_app
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Key Dependencies:**
- `snowflake-connector-python` - Snowflake database connectivity
- `requests` - HTTP client for Massive API calls
- `python-dotenv` - Environment variable management
- `schedule` - Job scheduling (optional, for scheduler.py)

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root (never commit this file):

```bash
# Massive API Configuration
POLYGON_API_KEY=your_massive_api_key_here

# Snowflake Configuration
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=xy12345.us-east-1  # Snowflake account identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH       # Compute warehouse name
SNOWFLAKE_ROLE=TRANSFORMER_ROLE      # Role with INSERT/CREATE privileges
SNOWFLAKE_DATABASE=MARKET_DATA       # Target database
SNOWFLAKE_SCHEMA=PUBLIC              # Target schema
SNOWFLAKE_TABLE=TICKERS              # Target table name
```

### .env.example Template

Commit `.env.example` with placeholder values for documentation:

```bash
POLYGON_API_KEY=mk_live_xxxxxxxxxxxxxxxx
SNOWFLAKE_USER=dbt_user
SNOWFLAKE_PASSWORD=***
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=TRANSFORMER_ROLE
SNOWFLAKE_DATABASE=MARKET_DATA
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_TABLE=TICKERS
```

## 📊 Usage

### Basic Execution

Run the ticker ingestion script:

```bash
python script.py
```

**Expected Output:**
```
Connecting to Snowflake...
Connected to Snowflake successfully!
Table TICKERS created or already exists
Fetching tickers from Massive API...
Fetched 10,000 tickers. Ingesting into Snowflake...
Ingested batch 1 (1000 records)
Ingested batch 2 (1000 records)
...
Successfully ingested 10,000 tickers into TICKERS
Snowflake connection closed
```

### Scheduled Execution

Use `scheduler.py` for automated daily runs:

```bash
python scheduler.py
```

Configure the schedule in `scheduler.py` to run at your preferred time:

```python
schedule.every().day.at("02:00").do(fetch_and_ingest_tickers_to_snowflake)
```

## 📁 Project Structure

```
stock_trading_python_app/
├── script.py                 # Main ETL pipeline
├── scheduler.py              # Scheduled job orchestrator
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variable template
├── .gitignore                # Git exclusion rules
├── LICENSE                   # MIT License
└── README.md                 # Project documentation
```

## 📊 Data Schema

The pipeline creates a `TICKERS` table with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `TICKER` | VARCHAR(20) | Stock symbol (e.g., AAPL) |
| `NAME` | VARCHAR(500) | Company/Security name |
| `MARKET` | VARCHAR(50) | Market type (stocks, options, etc.) |
| `LOCALE` | VARCHAR(10) | Geographic locale (us, gb, etc.) |
| `PRIMARY_EXCHANGE` | VARCHAR(10) | Primary listing exchange (NYSE, NASDAQ) |
| `TYPE` | VARCHAR(20) | Security type (ETF, FUND, STOCK) |
| `ACTIVE` | BOOLEAN | Whether ticker is currently active |
| `CURRENCY_NAME` | VARCHAR(50) | Trading currency (usd, eur) |
| `CIK` | VARCHAR(20) | SEC Central Index Key |
| `COMPOSITE_FIGI` | VARCHAR(50) | Bloomberg Composite FIGI identifier |
| `SHARE_CLASS_FIGI` | VARCHAR(50) | Bloomberg Share Class FIGI |
| `LAST_UPDATED_UTC` | TIMESTAMP | Last API update timestamp |
| `DS` | DATE | **Partitioning column** (daily snapshot date) |

**Example Record:**
```
TICKER: JLQD
NAME: Janus Henderson Corporate Bond ETF
MARKET: stocks
LOCALE: us
PRIMARY_EXCHANGE: ARCX
TYPE: ETF
ACTIVE: true
CURRENCY_NAME: usd
CIK: 0001500604
COMPOSITE_FIGI: BBG012FDKHT8
SHARE_CLASS_FIGI: BBG012FDKJP8
LAST_UPDATED_UTC: 2026-02-07 07:07:28
DS: 2026-02-07
```

## ⚠️ Error Handling & Rate Limiting

### Rate Limiting Strategy

The pipeline respects Massive API's rate limits:

```python
RATE_LIMIT_DELAY = 0.2  # 200ms between requests
```

**Features:**
- Automatic 200ms delay between paginated API calls
- 5-second backoff on HTTP 429 (Too Many Requests) errors
- Graceful retry mechanism with error detection

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| `Invalid API Key` | Verify `POLYGON_API_KEY` in `.env` file contains valid Massive API key |
| `Snowflake Connection Failed` | Check credentials and network connectivity to Snowflake |
| `Rate limit exceeded` | Pipeline auto-retries; monitor batch ingestion logs |
| `Column mismatch error` | Verify `COLUMN_MAPPING` aligns with Massive API response schema |

## 📈 Monitoring & Scheduling

### Using Cron (macOS/Linux)

Add to crontab for daily 2 AM run:

```bash
crontab -e

# Add this line:
0 2 * * * cd /path/to/stock_trading_python_app && source venv/bin/activate && python script.py >> logs/pipeline.log 2>&1
```

### Using Task Scheduler (Windows)

Create a scheduled task that runs:
```
python script.py
```

### Logging Best Practices

Enhance script.py with logging:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
```

## 🔧 Performance Tuning

**Customize batch size** (script.py):
```python
batch_size = 500  # Increase for faster ingestion on larger warehouses
```

**Adjust rate limit delay:**
```python
RATE_LIMIT_DELAY = 0.1  # For higher API tier limits
```

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

The MIT License is permissive and allows:
- ✅ Commercial use
- ✅ Modification
- ✅ Distribution
- ✅ Private use

**Conditions:**
- Include original license and copyright notice in distributions

## 📝 Version History

- **v1.0.0** (Feb 2026) - Initial ETL pipeline with Snowflake integration

## 🤝 Contributing

Contributions welcome! Please:

1. Create a feature branch (`git checkout -b feature/your-feature`)
2. Commit changes (`git commit -m 'Add feature'`)
3. Push to branch (`git push origin feature/your-feature`)
4. Open a Pull Request

## 📧 Support

For issues or questions:
- Check existing GitHub Issues
- Review Massive API docs: https://massive.com/docs
- Consult Snowflake docs: https://docs.snowflake.com

---

**Last Updated:** February 22, 2026