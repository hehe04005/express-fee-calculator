# Data Collection Scripts

Automated data synchronization system for agricultural e-commerce platform, integrating DingTalk API with MySQL database.

## Business Impact
- **Processing Time**: Reduced from 8 hours to 30 minutes (93.75% improvement)
- **Error Rate**: Decreased from 5-10% to <1%
- **Scale**: Handles 3.15 million orders annually (60% of China's seed market)
- **Cost Savings**: Eliminated 1 full-time employee's workload

## Scripts

### `dingtalk_exception_orders_sync.py`
**Customer service exception handling**
- Multi-source data integration (DingTalk, ERP, logistics APIs)
- Chunked reading with exponential backoff retry
- Complex business logic for geographic pricing rules

### `dingtalk_shipped_refunds_sync.py` 
**Refund processing with enhanced data handling**
- Row-level indexing to preserve data order
- Multi-format date parsing (supports 2025 data)
- Comprehensive data quality validation

### `dingtalk_seed_issues_sync.py`
**Agricultural quality control tracking**
- Domain-specific seed variety classification
- Customer complaint resolution workflow
- Regional quality distribution analysis

## Technical Features

**Data Processing**
- Robust error handling with bisection fallback
- Transaction-safe database operations
- Real-time data validation and cleaning

**Integration**
- DingTalk API integration with rate limiting
- MySQL connection with automatic retry
- Pandas-based data transformation pipeline

**Architecture**
```
DingTalk Documents → Python Scripts → Data Validation → MySQL Database
```

## Technologies
- Python, pandas, SQLAlchemy
- DingTalk API SDK
- MySQL with transaction management

## Configuration
Replace placeholder values in each script:
```python
DINGTALK_APP_CONFIG = {
    "app_key": "YOUR_DINGTALK_APP_KEY",
    "app_secret": "YOUR_DINGTALK_APP_SECRET"
}

DB_CONFIG = {
    'host': 'YOUR_DATABASE_HOST',
    'user': 'YOUR_DATABASE_USER',
    'password': 'YOUR_DATABASE_PASSWORD'
}
```

## Usage
```bash
pip install requests pandas sqlalchemy pymysql alibabacloud-dingtalk
python dingtalk_exception_orders_sync.py
```

---
*Demonstrates end-to-end data pipeline development with enterprise-scale reliability and agricultural domain expertise.*
