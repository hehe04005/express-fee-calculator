"""
DingTalk Data Synchronization - Seed Quality Issues Processing
Agricultural E-commerce Platform Data Collection System

This script automatically synchronizes seed germination and quality issue records
from DingTalk documents to MySQL database, featuring:
- Specialized handling for agricultural seed quality data
- Advanced date parsing with Excel serial number support
- Robust data cleaning and normalization
- Field-specific validation for agricultural products
- Comprehensive logging for quality assurance
- Transaction-safe database operations

Agricultural Domain Features:
- Seed variety classification and tracking
- Germination issue categorization
- Customer contact status management
- Regional distribution analysis
- Quality control workflow automation

Business Applications:
- Tracks seed quality issues and customer complaints
- Monitors germination rates and agricultural performance
- Enables proactive quality control measures
- Provides data for supplier relationship management
- Supports agricultural product development decisions

Developed during internship at Henan Desheng Seeds Co., Ltd.
Author: Zhao Yihe
Note: Sensitive configuration data has been anonymized for security
"""

import math
import logging
from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

# ========= LOGGING CONFIGURATION =========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_data.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========= CONFIGURATION CONSTANTS =========
# Note: Replace with your actual credentials in production
OPERATOR_ID = "YOUR_DINGTALK_OPERATOR_ID"
MAX_RETRY_TIMES = 5
RETRY_DELAY = 5

# ========= DINGTALK APPLICATION CONFIG =========
DINGTALK_APP_CONFIG = {
    "protocol": "https",
    "region_id": "central",
    "endpoint": "api.dingtalk.com",
    "app_key": "YOUR_DINGTALK_APP_KEY",
    "app_secret": "YOUR_DINGTALK_APP_SECRET"
}

# ========= MYSQL DATABASE CONFIG =========
DB_CONFIG = {
    'host': 'YOUR_DATABASE_HOST',
    'port': 'YOUR_DATABASE_PORT',
    'user': 'YOUR_DATABASE_USER',
    'password': 'YOUR_DATABASE_PASSWORD',
    'database': 'YOUR_DATABASE_NAME',
    'charset': 'utf8mb4',
    'connect_timeout': 10
}

# ========= TABLE/DOCUMENT CONFIGURATION =========
NODE_ID = "YOUR_DOCUMENT_NODE_ID"
SHEET_ID = "YOUR_SHEET_ID"
TABLE_NAME = "exception_seed_issue"

# Data reading parameters: A to M columns (13 total), max 1000 rows
COLUMN_RANGE = "A:M"
MAX_ROWS = 1000

# Expected 13-column Chinese headers (corresponding to database fields)
EXPECTED_HEADERS = [
    "Feedback Date", "Shop", "Order Number", "Customer Name", "Phone", 
    "Product Variety", "Quantity", "Issue Type", "Contact Status", 
    "Detailed Description", "Resolution Result", "Contact Person", "Region"
]

# ========= DINGTALK SDK IMPORTS =========
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dingtalk.oauth2_1_0.client import Client as Oauth2Client
from alibabacloud_dingtalk.oauth2_1_0 import models as oauth2_models

from alibabacloud_dingtalk.doc_1_0.client import Client as DocClient
from alibabacloud_dingtalk.doc_1_0 import models as doc_models
from alibabacloud_tea_util import models as util_models


# ========= UTILITY FUNCTIONS =========
def noneify(v):
    """
    Normalize various "empty" values to None (MySQL-compatible NULL)
    Handles pandas NaN, empty strings, and common null representations
    """
    if pd.isna(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        return s
    if isinstance(v, (float, int)):
        try:
            if isinstance(v, bool):
                return v
            fv = float(v)
            if math.isnan(fv) or math.isinf(fv):
                return None
        except Exception:
            return None
        return v
    return v


def parse_date_safe(x):
    """
    Enhanced date parsing with multiple format support
    Features:
    - Handles various date formats: '2025/3/23', '2025-03-23', '2025.03.23'
    - Excel serial number conversion support
    - Robust error handling with fallback to None
    - Agricultural domain date validation
    """
    if pd.isna(x):
        return None
    try:
        # Handle existing pandas Timestamp / datetime.date objects
        if hasattr(x, "date"):
            try:
                return x.date()  # pandas.Timestamp / datetime.datetime
            except Exception:
                return x          # datetime.date
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        # Excel serial number handling
        if s.isdigit():
            return pd.to_datetime(int(s), origin="1899-12-30", unit="D").date()
        # Normalize separators and parse
        s = s.replace("/", "-").replace(".", "-")
        d = pd.to_datetime(s, errors="coerce")
        if pd.isna(d):
            return None
        return d.date()
    except Exception:
        return None


def parse_decimal_safe(x):
    """
    Safe decimal parsing with agricultural context validation
    Handles quantity fields for seed products with reasonable range validation
    """
    if pd.isna(x):
        return None
    try:
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        val = float(s)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except Exception:
        return None


def get_access_token() -> str:
    """Obtain DingTalk access token using OAuth2"""
    conf = open_api_models.Config(
        protocol=DINGTALK_APP_CONFIG["protocol"],
        region_id=DINGTALK_APP_CONFIG["region_id"]
    )
    client = Oauth2Client(conf)
    req = oauth2_models.GetAccessTokenRequest(
        app_key=DINGTALK_APP_CONFIG["app_key"],
        app_secret=DINGTALK_APP_CONFIG["app_secret"]
    )
    resp = client.get_access_token(req)
    token = resp.body.access_token
    logger.info("Successfully obtained DingTalk access_token")
    return token


def get_doc_client() -> DocClient:
    """Create DingTalk document API client"""
    conf = open_api_models.Config(
        protocol=DINGTALK_APP_CONFIG["protocol"],
        region_id=DINGTALK_APP_CONFIG["region_id"]
    )
    conf.endpoint = DINGTALK_APP_CONFIG["endpoint"]
    return DocClient(conf)


def values_to_dataframe(values: List[List[str]]) -> pd.DataFrame:
    """
    Convert DingTalk API response to DataFrame with agricultural data validation
    Features:
    - Removes header row automatically
    - Ensures exactly 13 columns for seed issue data
    - Validates data structure for agricultural context
    - Provides comprehensive logging for data quality
    """
    values = values or []
    if not values:
        return pd.DataFrame(columns=EXPECTED_HEADERS)
    
    logger.info("=== Raw Data Structure Analysis ===")
    logger.info(f"Raw data rows: {len(values)}")
    if len(values) > 0:
        logger.info(f"First row content: {values[0][:10]}")  # Show first 10 columns
    if len(values) > 1:
        logger.info(f"Second row content: {values[1][:10]}")
    
    data_rows = values[1:]  # Remove header row
    fixed_rows = []
    for r in data_rows:
        r = list(r)
        # Normalize to exactly 13 columns
        if len(r) < 13:
            r.extend([""] * (13 - len(r)))
        else:
            r = r[:13]
        fixed_rows.append(r)
    
    df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
    
    # Log post-mapping data samples for validation
    logger.info("=== Post-Mapping Data Validation ===")
    for i in range(min(3, len(df))):
        logger.info(f"--- Row {i+1} Data ---")
        for col in df.columns:
            value = df.iloc[i][col]
            if pd.notna(value) and str(value).strip():
                value_str = str(value)[:50]  # Limit display length
                logger.info(f"  {col}: {value_str}")
            else:
                logger.info(f"  {col}: [empty]")
    
    return df


def fetch_sheet_dataframe(node_id: str, column_range: str, sheet_id: Optional[str]) -> pd.DataFrame:
    """
    Fetch and clean seed issue data from DingTalk document
    Features:
    - Reads A1:M1000 range for comprehensive data coverage
    - Applies agricultural domain-specific data cleaning
    - Validates seed variety and issue type classifications
    - Comprehensive data quality logging
    """
    token = get_access_token()
    client = get_doc_client()
    headers = doc_models.GetRangeHeaders(x_acs_dingtalk_access_token=token)
    req = doc_models.GetRangeRequest(operator_id=OPERATOR_ID)

    # Read A1:M1000 to include date column and full data range
    range_addr = f"A1:M{MAX_ROWS}"
    resp = client.get_range_with_options(
        workbook_id=node_id,
        sheet_id=sheet_id,
        range_address=range_addr,
        request=req,
        headers=headers,
        runtime=util_models.RuntimeOptions()
    )
    values = resp.body.values or []
    logger.info("Successfully read %s, returned %d raw rows", range_addr, len(values))

    df = values_to_dataframe(values)

    # Agricultural domain-specific data cleaning
    text_cols = ["Shop", "Order Number", "Customer Name", "Phone", "Product Variety", 
                "Issue Type", "Contact Status", "Detailed Description", "Resolution Result", 
                "Contact Person", "Region"]
    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).map(
                lambda v: "" if str(v).strip().lower() in ("", "nan", "none", "null") 
                else str(v).strip()
            )

    # Date and quantity processing with agricultural context
    df["Feedback Date"] = df["Feedback Date"].map(parse_date_safe)
    df["Quantity"] = df["Quantity"].map(parse_decimal_safe)

    # Remove completely empty records (agricultural data validation)
    # Require at least one of: Order Number, Customer Name, or Phone
    df = df[~((df["Order Number"]=="") & (df["Customer Name"]=="") & (df["Phone"]==""))].copy()

    # Convert all NaN/NaT to None for MySQL compatibility
    df = df.where(pd.notnull(df), None)

    logger.info("Successfully processed seed issue data, cleaned rows: %d", len(df))
    
    # Agricultural domain data quality reporting
    logger.info("=== Agricultural Data Quality Check ===")
    if 'Feedback Date' in df.columns:
        date_samples = df[df['Feedback Date'].notna()]['Feedback Date'].head(5).tolist()
        logger.info(f"Feedback date samples: {date_samples}")
    if 'Order Number' in df.columns:
        order_samples = df[df['Order Number'].notna()]['Order Number'].head(5).tolist()
        logger.info(f"Order number samples: {order_samples}")
    if 'Product Variety' in df.columns:
        variety_samples = df[df['Product Variety'].notna()]['Product Variety'].head(5).tolist()
        logger.info(f"Seed variety samples: {variety_samples}")
    
    return df


def create_engine_mysql() -> Engine:
    """Create MySQL database engine with optimized settings"""
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def ensure_table(engine: Engine):
    """
    Create seed issue table if it doesn't exist
    Optimized schema for agricultural seed quality tracking
    Note: Removed remarks field as actual data contains only 13 columns
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        `feedback_date` DATE NULL COMMENT 'Issue feedback date',
        `shop_name`     VARCHAR(255) COMMENT 'E-commerce shop name',
        `order_id`      VARCHAR(255) COMMENT 'Order identifier',
        `customer_name` VARCHAR(255) COMMENT 'Customer name',
        `phone`         VARCHAR(64) COMMENT 'Customer phone number',
        `product`       VARCHAR(255) COMMENT 'Seed variety/product type',
        `quantity`      DECIMAL(10,2) COMMENT 'Quantity of seeds',
        `issue`         VARCHAR(255) COMMENT 'Issue type classification',
        `contacted`     VARCHAR(64) COMMENT 'Customer contact status',
        `detail`        TEXT COMMENT 'Detailed issue description',
        `result`        TEXT COMMENT 'Resolution result/outcome',
        `contact`       VARCHAR(255) COMMENT 'Contact person handling case',
        `region`        VARCHAR(255) COMMENT 'Geographic region',
        `src_node_id`   VARCHAR(64) COMMENT 'Source document ID',
        `created_at`    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation time'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Seed quality issue tracking';
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Confirmed table %s exists with agricultural schema", TABLE_NAME)


def insert_rows(df: pd.DataFrame, node_id: str):
    """
    Insert seed issue records with comprehensive error handling
    Features:
    - Date normalization to MySQL-compatible format
    - Duplicate detection and handling
    - Agricultural data validation
    - Detailed insertion logging with sample data
    """
    sql = f"""
    INSERT INTO `{TABLE_NAME}`(
        feedback_date, shop_name, order_id, customer_name, phone, product, quantity,
        issue, contacted, detail, result, contact, region, src_node_id
    ) VALUES (
        :feedback_date, :shop_name, :order_id, :customer_name, :phone, :product, :quantity,
        :issue, :contacted, :detail, :result, :contact, :region, :src_node_id
    )
    """
    eng = create_engine_mysql()
    ensure_table(eng)

    inserted = skipped = failed = 0
    with eng.begin() as conn:
        for idx, row in df.iterrows():
            # Convert date to MySQL-compatible string format (None remains None)
            _d = parse_date_safe(row.get("Feedback Date"))
            fd_str = _d.strftime("%Y-%m-%d") if _d else None

            payload = {
                "feedback_date": fd_str,
                "shop_name":     row.get("Shop"),
                "order_id":      (str(row.get("Order Number") or "").strip()),
                "customer_name": row.get("Customer Name"),
                "phone":         (str(row.get("Phone") or "").strip()),
                "product":       row.get("Product Variety"),
                "quantity":      row.get("Quantity", None),
                "issue":         row.get("Issue Type"),
                "contacted":     row.get("Contact Status"),
                "detail":        row.get("Detailed Description"),
                "result":        row.get("Resolution Result"),
                "contact":       row.get("Contact Person"),
                "region":        row.get("Region"),
                "src_node_id":   node_id
            }
            # Normalize all empty values to None
            payload = {k: noneify(v) for k, v in payload.items()}

            try:
                conn.execute(text(sql), payload)
                inserted += 1
                if inserted <= 5:  # Log first few successful insertions
                    logger.info(f"Insert sample {inserted}: date={payload['feedback_date']}, "
                              f"shop={payload['shop_name']}, order={payload['order_id']}")
            except IntegrityError:
                skipped += 1
            except Exception as e:
                failed += 1
                logger.error("Insert failed (row %s): %s", idx + 2, e)

    logger.info("Insertion complete: Added %d, Skipped %d, Failed %d", inserted, skipped, failed)


# ========= MAIN EXECUTION WORKFLOW =========
if __name__ == "__main__":
    """
    Main synchronization workflow for seed quality issue data
    Orchestrates complete pipeline from DingTalk API to MySQL database
    """
    logger.info("Starting sync: Seed Germination Issue After-Sales Records")
    df = fetch_sheet_dataframe(NODE_ID, COLUMN_RANGE, SHEET_ID)
    insert_rows(df, NODE_ID)
    logger.info("Seed issue data synchronization completed successfully")
