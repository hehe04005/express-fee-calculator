# Express Fee Calculator

## Project Overview
Agricultural e-commerce logistics cost automation system developed during internship at China's largest seed e-commerce company.

## Technical Stack
- Python
- Flask
- Data API Integration
- Pandas & NumPy

## Business Impact
- **Processing Time**: 8 hours → 30 minutes (93.75% reduction)
- **Error Rate**: 5-10% → <1%
- **Processing Scale**: 3.15 million orders annually
- **Market Coverage**: 60% of China's seed sales market

## How to Run
```bash
pip install -r requirements.txt
python app.py
```

## System Architecture
```
Web Application (Flask) → Data Processing → Business Logic → Database
     ↑                          ↑                ↑            ↓
Data Collection Scripts → API Integration → Data Cleaning → MySQL
     ↑                          ↑                ↑            ↑
DingTalk Documents     →    Python ETL    → Validation  → Real-time Sync
```

## Complete Solution Components

### 1. **Core Web Application**
- Flask-based logistics cost calculation interface
- Real-time processing with <1% error rate
- User-friendly interface for non-technical staff
- Cloud deployment with scalable architecture

### 2. **Data Collection Pipeline** 
Advanced data synchronization system handling enterprise-scale operations:

**Exception Orders Processing** (`data_collection/dingtalk_exception_orders_sync.py`)
- Customer service workflow automation
- Multi-source integration (DingTalk, ERP, logistics APIs)
- Chunked reading with exponential backoff retry mechanisms

**Shipped Refunds Management** (`data_collection/dingtalk_shipped_refunds_sync.py`)  
- Real-time refund tracking and analytics
- Enhanced data handling with order preservation
- Multi-format date parsing supporting 2025 operations

**Agricultural Quality Control** (`data_collection/dingtalk_seed_issues_sync.py`)
- Seed quality and germination issue tracking  
- Domain-specific agricultural data validation
- Regional quality distribution analysis

**Technical Features:**
- Robust error handling with bisection fallback algorithms
- Transaction-safe database operations
- Enterprise-grade API integration with rate limiting
- Comprehensive data validation and cleaning pipelines

See [`data_collection/`](./data_collection/) for detailed technical documentation.

## Core Features
- **Multi-source Data Integration**: DingTalk API, ERP Database, Logistics Providers
- **Complex Business Logic Modeling**: Geographic and product-specific pricing rules
- **Automated Processing & Deployment**: Flask web application with cloud deployment
- **Error Handling & Data Validation**: Robust retry mechanisms and quality controls

## Key Achievements
This project demonstrates end-to-end data science and software engineering capabilities:

**Core Application Impact:**
- **Processing Speed**: Reduced manual calculations from 8 hours to 30 minutes (93.75% improvement)
- **Accuracy**: Decreased error rates from 5-10% to <1%
- **User Experience**: Enabled non-technical staff to process enterprise-level datasets
- **Deployment**: Cloud-based scalable architecture

**Data Pipeline Impact:**  
- **Scale**: Automated processing of 3.15 million orders annually
- **Market Coverage**: Serving 60% of China's seed sales market
- **Cost Reduction**: Eliminated 1 full-time employee's monthly workload  
- **Integration**: Multi-platform data fusion (DingTalk, ERP, Logistics systems)

**Technical Excellence:**
- **System Design**: Complete data science lifecycle from collection to application
- **Domain Expertise**: Agricultural e-commerce specialized solutions
- **Enterprise Architecture**: Transaction-safe operations with comprehensive error handling
- **Business Intelligence**: Real-time analytics supporting management decisions

## Project Background
Data science solution developed during internship at Henan Desheng Seeds Co., Ltd., addressing inefficiencies in manual logistics cost calculations.

## Developer
**Zhao Yihe** - Data Science Graduate Student Applicant  
*Work Project at Henan Desheng Seeds Co., Ltd.*
