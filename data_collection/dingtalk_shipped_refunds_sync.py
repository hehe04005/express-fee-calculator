"""
DingTalk Data Synchronization - Shipped Refunds Processing
Agricultural E-commerce Platform Data Collection System

This script automatically synchronizes shipped refund data from DingTalk
documents to MySQL database with enhanced features:
- Improved date conversion logic to handle 2025 data accurately
- Maintains data order during chunked reading operations  
- Enhanced data cleaning with integrity preservation
- Row-level tracking to ensure original data sequence
- Comprehensive data quality validation and logging

Key Technical Features:
- Advanced chunked reading with bisection fallback
- Multi-format date parsing supporting various input formats
- Robust error handling with exponential backoff retry
- Data quality checks and validation at multiple stages
- Transaction-safe database operations

Business Applications:
- Tracks refund processing for shipped agricultural products
- Monitors compensation amounts and processing times
- Provides data for supply chain optimization decisions
- Enables real-time refund status reporting

Developed during internship at Henan Desheng Seeds Co., Ltd.
Author: Zhao Yihe
Note: Sensitive configuration data has been anonymized for security
"""

!pip install -q requests pandas sqlalchemy pymysql alibabacloud-tea-openapi alibabacloud-dingtalk alibabacloud-tea-util

import time
import requests
import logging
from datetime import datetime, timedelta
from typing import List
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from alibabacloud_dingtalk.doc_1_0.client import Client as DocClient
from alibabacloud_dingtalk.doc_1_0.models import (
    GetAllSheetsRequest, GetRangeRequest, GetSheetRequest,
    GetAllSheetsHeaders, GetRangeHeaders, GetSheetHeaders
)
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models

# ===== LOGGING CONFIGURATION =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("shipped_refunds_sync")

# ===== CONFIGURATION CONSTANTS =====
# Note: Replace with your actual credentials in production
OPERATOR_ID = "YOUR_DINGTALK_OPERATOR_ID"

DINGTALK_APP_CONFIG = {
    "protocol": "https",
    "region_id": "central",
    "endpoint": "api.dingtalk.com",
    "app_key": "YOUR_DINGTALK_APP_KEY",
    "app_secret": "YOUR_DINGTALK_APP_SECRET"
}

DB_CONFIG = {
    'host': 'YOUR_DATABASE_HOST',
    'user': 'YOUR_DATABASE_USER',
    'password': 'YOUR_DATABASE_PASSWORD',
    'database': 'YOUR_DATABASE_NAME',
    'charset': 'utf8mb4',
    'connect_timeout': 10
}

# Document configuration
NODE_ID = "YOUR_DOCUMENT_NODE_ID"
TARGET_DOC_NAME = "Shipped Refunds"

# Data processing parameters
READ_MAX_COLS = 14           # Read only A(1) to N(14) columns
MAX_CELLS_PER_CALL = 30000   # API rate limit
CHUNK_SLEEP_SEC = 0.30       # Delay between chunks
MAX_RETRIES_PER_CHUNK = 5    # Retry attempts per chunk
INITIAL_BACKOFF_SEC = 1.0    # Exponential backoff starting point
MIN_ROWS_PER_CHUNK = 30      # Minimum rows per chunk for bisection

# ===== DATA CLASSES =====
class DocumentInfo:
    """Document metadata container"""
    def __init__(self, node_id: str, name: str):
        self.node_id = node_id
        self.name = name

class SyncResult:
    """Synchronization operation result with metrics"""
    def __init__(self, node_id: str, doc_name: str, success: bool = False,
                 error_msg: str = "", inserted_count: int = 0, skipped_count: int = 0):
        self.node_id = node_id
        self.doc_name = doc_name
        self.success = success
        self.error_msg = error_msg
        self.inserted_count = inserted_count
        self.skipped_count = skipped_count

# ===== MAIN PROCESSOR CLASS =====
class ShippedRefundsDataSyncProcessor:
    """
    Enhanced data synchronization processor for shipped refunds
    Features improved data handling and validation for 2025 data
    """
    
    def __init__(self):
        self.access_token = None

    def get_tenant_access_token(self) -> str:
        """Obtain DingTalk access token using app credentials"""
        url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        payload = {
            "appKey": DINGTALK_APP_CONFIG["app_key"], 
            "appSecret": DINGTALK_APP_CONFIG["app_secret"]
        }
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        token = resp.json().get("accessToken")
        if not token:
            raise Exception(f"Failed to obtain access_token: {resp.text}")
        logger.info("Successfully obtained DingTalk access_token")
        return token

    def create_doc_client(self) -> DocClient:
        """Create DingTalk document API client"""
        cfg = open_api_models.Config()
        cfg.protocol = DINGTALK_APP_CONFIG["protocol"]
        cfg.region_id = DINGTALK_APP_CONFIG["region_id"]
        cfg.endpoint = DINGTALK_APP_CONFIG["endpoint"]
        return DocClient(cfg)

    @staticmethod
    def _col_letter(col_num: int) -> str:
        """Convert column number to Excel-style letter (1=A, 2=B, etc.)"""
        letters = []
        while col_num > 0:
            col_num, rem = divmod(col_num - 1, 26)
            letters.append(chr(65 + rem))
        return ''.join(reversed(letters))

    def get_document_data(self, node_id: str) -> pd.DataFrame:
        """
        Enhanced document data retrieval with improved order preservation
        Features:
        - Row-level indexing to maintain original data sequence
        - Enhanced bisection algorithm for failed chunks
        - Comprehensive data structure logging for debugging
        - Support for 2025 data with improved date handling
        """
        if not self.access_token:
            raise Exception("Access token not initialized")

        client = self.create_doc_client()

        # Get first sheet
        sheets_headers = GetAllSheetsHeaders(x_acs_dingtalk_access_token=self.access_token)
        sheets_resp = client.get_all_sheets_with_options(
            workbook_id=node_id,
            request=GetAllSheetsRequest(operator_id=OPERATOR_ID),
            headers=sheets_headers,
            runtime=util_models.RuntimeOptions()
        )
        if not sheets_resp.body.value:
            raise Exception(f"Document {node_id} has no worksheets")
        sheet_id = sheets_resp.body.value[0].id

        # Get sheet metadata
        sheet_headers = GetSheetHeaders(x_acs_dingtalk_access_token=self.access_token)
        meta = client.get_sheet_with_options(
            workbook_id=node_id,
            sheet_id=sheet_id,
            request=GetSheetRequest(operator_id=OPERATOR_ID),
            headers=sheet_headers,
            runtime=util_models.RuntimeOptions()
        ).body
        if meta.last_non_empty_column is None or meta.last_non_empty_row is None:
            raise Exception(f"Document {node_id} worksheet is empty")

        last_col_idx = min(int(meta.last_non_empty_column) + 1, READ_MAX_COLS)
        last_row_idx = int(meta.last_non_empty_row) + 1
        last_col_letter = self._col_letter(last_col_idx)
        total_cells = last_col_idx * last_row_idx
        logger.info(f"Reading first {last_col_idx} columns (A:{last_col_letter}), "
                   f"sheet size: {last_row_idx} rows × {last_col_idx} cols ≈ {total_cells} cells")

        # Calculate optimal chunk size
        max_rows_per_chunk = max(MIN_ROWS_PER_CHUNK, 
                               min(last_row_idx, MAX_CELLS_PER_CALL // max(1, last_col_idx)))
        logger.info(f"Using chunked reading with max {max_rows_per_chunk} rows per chunk...")

        range_headers = GetRangeHeaders(x_acs_dingtalk_access_token=self.access_token)

        def fetch_block(r1: int, r2: int):
            """Fetch a single block with retry logic"""
            addr = f"A{r1}:{last_col_letter}{r2}"
            for attempt in range(MAX_RETRIES_PER_CHUNK + 1):
                try:
                    resp = client.get_range_with_options(
                        workbook_id=node_id,
                        sheet_id=sheet_id,
                        range_address=addr,
                        request=GetRangeRequest(operator_id=OPERATOR_ID),
                        headers=range_headers,
                        runtime=util_models.RuntimeOptions()
                    )
                    return resp.body.values or []
                except Exception as e:
                    msg = str(e)
                    transient = ("ServiceUnavailable" in msg) or ("statusCode': 503" in msg) or \
                                ("TooManyRequests" in msg) or ("statusCode': 429" in msg)
                    if attempt < MAX_RETRIES_PER_CHUNK and transient:
                        sleep_s = INITIAL_BACKOFF_SEC * (2 ** attempt)
                        logger.warning(f"Block {addr} failed ({msg[:120]}...), "
                                     f"retrying in {sleep_s:.1f}s (attempt {attempt+1}/{MAX_RETRIES_PER_CHUNK})")
                        time.sleep(sleep_s)
                        continue
                    raise

        def fetch_block_with_bisect(r1: int, r2: int) -> list:
            """Enhanced bisection with order preservation"""
            try:
                return fetch_block(r1, r2)
            except Exception as e:
                rows = r2 - r1 + 1
                if rows <= MIN_ROWS_PER_CHUNK:
                    logger.error(f"Minimum block still failing: A{r1}:{last_col_letter}{r2}, error: {e}")
                    raise
                mid = (r1 + r2) // 2
                # Ensure proper order when combining results
                left = fetch_block_with_bisect(r1, mid)
                right = fetch_block_with_bisect(mid + 1, r2)
                return (left or []) + (right or [])

        # Enhanced chunked reading with row tracking
        all_rows = []
        chunks_info = []  # Track chunk information for debugging
        start = 1
        chunk_index = 0
        
        while start <= last_row_idx:
            end = min(last_row_idx, start + max_rows_per_chunk - 1)
            vals = fetch_block_with_bisect(start, end)
            if vals:
                # Debug: Log first chunk structure for validation
                if chunk_index == 0 and len(vals) > 0:
                    logger.info("=== First Chunk Raw Data Check ===")
                    for i in range(min(3, len(vals))):
                        logger.info(f"Raw row {start + i}: {vals[i]}")
                
                # Add original row numbers to preserve order
                for i, row in enumerate(vals):
                    row_with_index = [start + i] + (row if isinstance(row, list) else [row])
                    all_rows.append(row_with_index)
                chunks_info.append((chunk_index, start, end, len(vals)))
                
            logger.info(f"Read range A{start}:{last_col_letter}{end} complete, chunk has {len(vals) if vals else 0} rows")
            time.sleep(CHUNK_SLEEP_SEC)
            start = end + 1
            chunk_index += 1

        if not all_rows:
            raise Exception(f"Document {node_id} contains no data")

        logger.info(f"Read {len(chunks_info)} total chunks, {len(all_rows)} raw rows")
        
        # Sort by original row numbers to ensure correct order
        all_rows.sort(key=lambda x: x[0])
        
        # Remove row index column, restore original data structure
        sorted_rows = [row[1:] for row in all_rows]

        df = pd.DataFrame(sorted_rows)
        df = self.clean_data(df)
        logger.info(f"Successfully retrieved data, cleaned rows: {len(df)}")
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced data cleaning with improved DingTalk structure handling
        Features:
        - Accurate column mapping for agricultural e-commerce data
        - Enhanced date parsing supporting 2025 data
        - Improved text cleaning with length validation
        - Comprehensive data quality reporting
        """
        df = df.dropna(how='all')
        if df.empty:
            return df

        # Debug: Log raw data structure for validation
        logger.info("=== Raw Data Structure Analysis ===")
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {df.shape[1]}")
        logger.info("First 5 rows (showing first 10 columns only):")
        for i in range(min(5, len(df))):
            row_data = df.iloc[i].tolist()[:10]
            logger.info(f"Row {i+1} first 10 cols: {row_data}")
        
        # Log column-wise data samples
        logger.info("=== Column Data Samples ===")
        for col_idx in range(min(df.shape[1], 15)):
            sample_values = [str(df.iloc[i, col_idx])[:50] if not pd.isna(df.iloc[i, col_idx]) else 'NaN' 
                           for i in range(min(3, len(df)))]
            logger.info(f"Column {col_idx}: {sample_values}")

        # Map to actual DingTalk table structure
        actual_dingtalk_columns = [
            'Order Date',        # 0
            'Feedback Date',     # 1  
            'Shop',              # 2
            'Order Number',      # 3
            'Customer Name',     # 4
            'Phone',             # 5
            'Address',           # 6
            'Product',           # 7
            'Quantity',          # 8
            'Amount',            # 9
            'Original Tracking', # 10
            'Resend Tracking',   # 11
            'Issue Type',        # 12
            'Description'        # 13
        ]
        
        # Detect and handle header row
        has_header = False
        if len(df) > 0:
            first_row_str = [str(cell) for cell in df.iloc[0].tolist()]
            logger.info(f"First row content: {first_row_str[:10]}")
            
            # Check for DingTalk table header keywords
            dingtalk_keywords = ['Order Date', 'Feedback Date', 'Shop', 'Order Number', 'Product']
            matched_keywords = sum(1 for keyword in dingtalk_keywords if any(keyword in str(cell) for cell in first_row_str))
            
            logger.info(f"DingTalk header keywords matched: {matched_keywords}")
            
            if matched_keywords >= 3:  # Header detected
                has_header = True
                logger.info("Detected DingTalk header row, skipping first row")
                df = df.iloc[1:].reset_index(drop=True)
                logger.info(f"After header removal, row count: {len(df)}")
            else:
                logger.info("No standard header detected")
        
        # Apply DingTalk column structure
        if len(df.columns) >= 14:
            df.columns = actual_dingtalk_columns[:len(df.columns)]
            logger.info(f"Applied DingTalk column names: {list(df.columns)}")
        else:
            logger.warning(f"Insufficient columns: {len(df.columns)}, expected at least 14")
            
        # Create database field mapping
        dingtalk_to_db_mapping = {
            'Feedback Date': 'date',
            'Original Tracking': 'tracking_number',
            'Issue Type': 'refund_result',
            'Order Number': 'order_number', 
            'Product': 'product',
            'Quantity': 'quantity',
            'Amount': 'amount',
            'Description': 'remarks',
            'Shop': 'order_platform',
            'Customer Name': 'registrar',
            'Original Tracking': 'express'
        }
        
        # Create new dataframe with database schema
        db_df = pd.DataFrame()
        
        # Handle date field - prioritize Order Date for actual transaction date
        if 'Order Date' in df.columns:
            db_df['date'] = df['Order Date']
        elif 'Feedback Date' in df.columns:
            db_df['date'] = df['Feedback Date'] 
        else:
            db_df['date'] = None
            
        # Handle tracking number - prioritize Original Tracking
        if 'Original Tracking' in df.columns:
            db_df['tracking_number'] = df['Original Tracking']
        elif 'Resend Tracking' in df.columns:
            db_df['tracking_number'] = df['Resend Tracking'] 
        else:
            db_df['tracking_number'] = None
            
        # Map other fields directly
        field_mappings = [
            ('Issue Type', 'refund_result'),
            ('Order Number', 'order_number'),
            ('Product', 'product'), 
            ('Quantity', 'quantity'),
            ('Amount', 'amount'),
            ('Description', 'remarks'),
            ('Shop', 'order_platform'),
            ('Customer Name', 'registrar')
        ]
        
        for dingtalk_col, db_col in field_mappings:
            if dingtalk_col in df.columns:
                db_df[db_col] = df[dingtalk_col]
            else:
                db_df[db_col] = None
                
        # Express field using Original Tracking
        if 'Original Tracking' in df.columns:
            db_df['express'] = df['Original Tracking']
        else:
            db_df['express'] = None
            
        # Fields to be populated separately or left empty
        db_df['compensation_amount'] = None
        db_df['arrival_time'] = None
        
        # Replace original dataframe
        df = db_df
        
        logger.info(f"Remapped column names: {list(df.columns)}")
        
        # Log remapped data samples
        if len(df) > 0:
            logger.info("=== Post-Mapping Data Check ===")
            for i in range(min(3, len(df))):
                logger.info(f"--- Row {i+1} Data ---")
                for col in df.columns:
                    value = df.iloc[i][col]
                    if pd.notna(value) and str(value).strip():
                        value_str = str(value)[:100]
                        logger.info(f"  {col}: {value_str}")
                    else:
                        logger.info(f"  {col}: [empty]")

        # Ensure target database columns
        target_cols = ['date','tracking_number','refund_result','order_number','product','quantity','amount',
                      'remarks','order_platform','registrar','express','compensation_amount','arrival_time']
        df = df[target_cols]

        # Enhanced text cleaning
        def clean_text(x):
            """Enhanced text cleaning with length limits"""
            if pd.isna(x) or x in [None, "", " ", "nan", "NaN"]:
                return None
            text = str(x).strip()
            # Remove various newline characters
            text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
            # Remove extra spaces
            text = ' '.join(text.split())
            # Apply length limits based on database field constraints
            if len(text) > 500:
                text = text[:500]
                logger.warning(f"Text truncated due to length: {text}")
            return text if text else None

        # Apply text cleaning to relevant columns
        text_columns = ['tracking_number', 'refund_result', 'order_number', 'product', 
                       'remarks', 'order_platform', 'registrar', 'express']
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(clean_text)
                # Log cleaned samples
                cleaned_samples = df[df[col].notna()][col].head(3).tolist()
                logger.info(f"{col} cleaned samples: {cleaned_samples}")

        # Enhanced type conversion functions
        def to_int(x):
            try:
                if pd.isna(x) or x in [None, "", " ", "nan", "NaN"]: 
                    return None
                x_str = str(x).replace(',', '').strip()
                # Exclude non-numeric content (Chinese characters, long strings)
                if any('\u4e00' <= c <= '\u9fff' for c in x_str) or len(x_str) > 8:
                    return None
                # Process reasonable integer ranges (avoid phone numbers as quantities)
                if x_str.replace('.', '').replace('-', '').isdigit():
                    value = int(float(x_str))
                    # Reasonable quantity range: 1-10000
                    if 1 <= value <= 10000:
                        return value
                return None
            except: 
                return None

        def to_float(x):
            try:
                if pd.isna(x) or x in [None, "", " ", "nan", "NaN"]: 
                    return None
                x_str = str(x).replace(',', '').replace('¥', '').replace('￥', '').strip()
                # Exclude non-monetary content
                if any('\u4e00' <= c <= '\u9fff' for c in x_str) or len(x_str) > 12:
                    return None
                # Check if purely numeric
                if x_str.replace('.', '').replace('-', '').isdigit():
                    value = float(x_str)
                    # Reasonable amount range: 0-100000 (avoid phone numbers as amounts)
                    if 0 <= value <= 100000:
                        return value
                return None
            except: 
                return None

        def to_date(x):
            """Enhanced date conversion supporting 2025 data"""
            try:
                if pd.isna(x) or x in [None, "", " ", "nan", "NaN"]: 
                    return None
                s = str(x).strip()
                
                # Priority: standard date formats
                for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
                    try: 
                        parsed = datetime.strptime(s, fmt).date()
                        if 2020 <= parsed.year <= 2030:  # Support 2025 data
                            return parsed
                    except: 
                        pass
                
                # Handle Chinese date format
                if "年" in s and "月" in s and "日" in s:
                    try:
                        return datetime.strptime(s, "%Y年%m月%d日").date()
                    except: 
                        pass
                
                # Handle MM/DD format, default to 2025
                if "/" in s and len(s.split("/")) == 2:
                    try:
                        parts = s.split("/")
                        month, day = int(parts[0]), int(parts[1])
                        return datetime(2025, month, day).date()
                    except: 
                        pass
                
                # Handle YYYY/M/D format
                if "/" in s and len(s.split("/")) == 3:
                    try:
                        parts = s.split("/")
                        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                        if 2020 <= year <= 2030:
                            return datetime(year, month, day).date()
                    except: 
                        pass
                
                # Excel serial number conversion (2025 range support)
                if s.isdigit() and 40000 <= int(s) <= 50000:
                    try:
                        base = datetime(1900, 1, 1)
                        result = (base + timedelta(days=int(s) - 2)).date()
                        if 2020 <= result.year <= 2030:
                            return result
                    except: 
                        pass
                
                return None
            except: 
                return None

        # Apply type conversions
        if 'quantity' in df.columns: 
            df['quantity'] = df['quantity'].apply(to_int)
        for c in ['amount', 'compensation_amount']:
            if c in df.columns: 
                df[c] = df[c].apply(to_float)
        for c in ['date', 'arrival_time']:
            if c in df.columns: 
                df[c] = df[c].apply(to_date)

        # Remove rows where key fields are all empty
        key_cols = [c for c in ['tracking_number', 'order_number', 'product', 'refund_result'] if c in df.columns]
        if key_cols: 
            df = df.dropna(how='all', subset=key_cols)

        # Convert NaN/NaT to None for MySQL compatibility
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)

        # Data quality reporting
        logger.info("=== Data Quality Check ===")
        if 'date' in df.columns:
            date_samples = df[df['date'].notna()]['date'].head(5).tolist()
            logger.info(f"Date samples: {date_samples}")
        if 'tracking_number' in df.columns:
            tracking_samples = df[df['tracking_number'].notna()]['tracking_number'].head(5).tolist()
            logger.info(f"Tracking number samples: {tracking_samples}")
        if 'refund_result' in df.columns:
            refund_samples = df[df['refund_result'].notna()]['refund_result'].head(5).tolist()
            logger.info(f"Refund result samples: {refund_samples}")
        if 'product' in df.columns:
            product_samples = df[df['product'].notna()]['product'].head(5).tolist()
            logger.info(f"Product samples: {product_samples}")
        if 'order_platform' in df.columns:
            platform_samples = df[df['order_platform'].notna()]['order_platform'].head(5).tolist()
            logger.info(f"Order platform samples: {platform_samples}")
            
        # Final validation: log complete sample rows
        logger.info("=== Final Data Validation ===")
        for i in range(min(2, len(df))):
            row_dict = {}
            for col in df.columns:
                row_dict[col] = df.iloc[i][col]
            logger.info(f"Final row {i+1} data: {row_dict}")

        return df

    def create_db_engine(self):
        """Create SQLAlchemy database engine"""
        conn = (f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
               f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}")
        return create_engine(conn, connect_args={'connect_timeout': DB_CONFIG['connect_timeout']})

    def insert_shipped_refunds(self, df: pd.DataFrame, connection, node_id: str):
        """
        Enhanced database insertion with comprehensive validation
        Features detailed logging and sample data tracking
        """
        sql = """
        INSERT INTO shipped_refunds(
            date, tracking_number, refund_result, order_number, product, quantity, 
            amount, remarks, order_platform, registrar, express, compensation_amount, arrival_time
        ) VALUES (
            :date, :tracking_number, :refund_result, :order_number, :product, :quantity, 
            :amount, :remarks, :order_platform, :registrar, :express, :compensation_amount, :arrival_time
        )
        """
        ins, skip, err = 0, 0, 0
        
        logger.info(f"Starting insertion of {len(df)} rows...")
        
        for idx, row in df.iterrows():
            # Skip rows where primary fields are all empty
            if not any([row.get('tracking_number'), row.get('order_number'), row.get('product')]):
                continue
                
            data = {}
            for col in ['date', 'tracking_number', 'refund_result', 'order_number', 'product', 'quantity', 
                       'amount', 'remarks', 'order_platform', 'registrar', 'express', 'compensation_amount', 'arrival_time']:
                data[col] = row.get(col, None)
            
            try:
                connection.execute(text(sql), [data])
                ins += 1
                if ins <= 5:  # Log first few successful insertions
                    logger.info(f"Insert sample {ins}: date={data.get('date')}, tracking={data.get('tracking_number')}, product={data.get('product')}")
            except IntegrityError as e:
                skip += 1
                if skip <= 3:
                    logger.warning(f"Skipped duplicate: tracking_number={data.get('tracking_number')}")
            except Exception as e:
                err += 1
                if err <= 5:  # Log first few errors only
                    logger.error(f"Insert failed {err}: {e}")
                    logger.error(f"Failed data sample: {data}")
                    
        logger.info(f"Insertion complete - Added: {ins} records; Skipped duplicates: {skip}; Errors: {err}")
        return ins, skip

    def _sync_document_data_once(self, doc_info: DocumentInfo) -> SyncResult:
        """Synchronize single document with enhanced logging"""
        logger.info(f"Starting sync for document: {doc_info.name}")
        df = self.get_document_data(doc_info.node_id)
        
        # Additional safety: ensure null value consistency
        df = df.astype(object).where(pd.notnull(df), None)

        engine = self.create_db_engine()
        # Transaction-safe insertion
        with engine.begin() as conn:
            inserted, skipped = self.insert_shipped_refunds(df, conn, doc_info.node_id)
            
        logger.info(f"Document {doc_info.name} sync complete")
        return SyncResult(doc_info.node_id, doc_info.name, True, "", inserted, skipped)

    def run_sync_process(self):
        """
        Main synchronization orchestration
        Enhanced with comprehensive reporting and monitoring
        """
        try:
            logger.info("Starting sync: Shipped Refunds Data")
            self.access_token = self.get_tenant_access_token()
            
            docs = [DocumentInfo(NODE_ID, TARGET_DOC_NAME)]
            total_i, total_s = 0, 0
            
            for d in docs:
                r = self._sync_document_data_once(d)
                total_i += r.inserted_count
                total_s += r.skipped_count
                logger.info(f"{r.doc_name}: Added {r.inserted_count}, Skipped {r.skipped_count}")
                time.sleep(0.30)
                
            logger.info("=== Synchronization Summary ===")
            logger.info(f"Documents: {len(docs)} | Added: {total_i} | Skipped: {total_s}")
            logger.info("Data synchronization complete. Please verify data order and content in database")
            
        except Exception as e:
            logger.error(f"Synchronization error: {e}")
            raise

# ===== EXECUTION =====
if __name__ == "__main__":
    processor = ShippedRefundsDataSyncProcessor()
    processor.run_sync_process()
