#!/usr/bin/env python3
"""
SECURE MCP (Model Context Protocol) Server for File Query Operations - Web/HTTP Version
This server provides tools to discover, load, and query data files using SQL
Supports CSV, JSON, Excel, and Parquet file formats from AWS S3
Designed for deployment on Render.com with HTTP/SSE transport

SECURITY FEATURES:
- API Key authentication
- Environment variable credential handling
- Request rate limiting
- CORS protection
"""

import sys
import os
import asyncio
from typing import Any, Optional, Dict


# Add current directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    from mcp.server.fastmcp import FastMCP
    print("FastMCP imported successfully")
except ImportError as e:
    print(f"Failed to import FastMCP: {e}")
    print(f"Python path: {sys.path}")
    sys.exit(1)

from typing import Any, Optional
import httpx
import json
import mcp
import duckdb
import polars as pl
import gc
import pandas as pd
import pyarrow
import pydantic
from pydantic import BaseModel, Field
from typing import Dict
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import tempfile
import urllib.parse
from pathlib import Path
from datetime import datetime

# Web server imports for HTTP transport
try:
    from fastapi import FastAPI, Request, HTTPException, Depends, Security, UploadFile, File
    from fastapi.responses import StreamingResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    import uvicorn
    print("FastAPI imported successfully")
except ImportError as e:
    print(f"Failed to import FastAPI: {e}")
    print("Installing required web dependencies...")
    os.system("pip install fastapi uvicorn")
    from fastapi import FastAPI, Request, HTTPException, Depends, Security, UploadFile, File
    from fastapi.responses import StreamingResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    import uvicorn

# ============================================================================
# SECURITY CONFIGURATION
# ============================================================================

# Security settings - these should be set via environment variables
API_KEY = os.getenv('MCP_API_KEY', 'your-secure-api-key-here')  # Change this!

security = HTTPBearer()

def verify_api_key(credentials: HTTPAuthorizationCredentials = Security(security)):
    """Verify API key authentication"""
    if credentials.credentials != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return credentials.credentials



# ============================================================================
# ENVIRONMENT VARIABLE HANDLING FOR RENDER DEPLOYMENT
# ============================================================================

def get_env_with_fallbacks(key: str, default: str = None) -> str:
    """Get environment variable with multiple fallback strategies for Render deployment"""
    # Try direct environment variable
    value = os.getenv(key)
    if value:
        return value
    
    # Try with different case variations
    value = os.getenv(key.upper())
    if value:
        return value
        
    value = os.getenv(key.lower())
    if value:
        return value
    
    # Return default if nothing found
    return default

# Get S3 configuration from environment variables
# These will be set in Render.com environment variables
S3_BUCKET_NAME = get_env_with_fallbacks('S3_BUCKET_NAME', 'your-bucket-name')
S3_FOLDER_PATH = get_env_with_fallbacks('S3_FOLDER_PATH', 'your-folder-path')
S3_REGION = get_env_with_fallbacks('AWS_DEFAULT_REGION', 'us-east-1')

# Full S3 path for your data
S3_DATA_PATH = f"s3://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}"

print(f"Environment Configuration:")
print(f"  S3_BUCKET_NAME: {S3_BUCKET_NAME}")
print(f"  S3_FOLDER_PATH: {S3_FOLDER_PATH}")
print(f"  S3_REGION: {S3_REGION}")
print(f"  API_KEY: {'SET' if API_KEY != 'your-secure-api-key-here' else 'NOT SET (SECURITY RISK!)'}")

# Initialize AWS S3 client using environment variables
try:
    # Check if AWS credentials are available in environment
    aws_access_key = get_env_with_fallbacks('AWS_ACCESS_KEY_ID')
    aws_secret_key = get_env_with_fallbacks('AWS_SECRET_ACCESS_KEY')
    
    if aws_access_key and aws_secret_key:
        print(f"AWS credentials found in environment variables")
        s3_client = boto3.client('s3', 
                                aws_access_key_id=aws_access_key,
                                aws_secret_access_key=aws_secret_key,
                                region_name=S3_REGION)
        s3_resource = boto3.resource('s3', 
                                   aws_access_key_id=aws_access_key,
                                   aws_secret_access_key=aws_secret_key,
                                   region_name=S3_REGION)
        S3_AVAILABLE = True
        print(f"S3 Client initialized for bucket: {S3_BUCKET_NAME}")
        print(f"Data folder: {S3_FOLDER_PATH}")
        print(f"Region: {S3_REGION}")
    else:
        print(f"AWS credentials not found in environment variables")
        print(f"  AWS_ACCESS_KEY_ID: {'SET' if aws_access_key else 'NOT SET'}")
        print(f"  AWS_SECRET_ACCESS_KEY: {'SET' if aws_secret_key else 'NOT SET'}")
        print(f"  Please set AWS credentials in Render environment variables")
        s3_client = None
        s3_resource = None
        S3_AVAILABLE = False
        
except (NoCredentialsError, Exception) as e:
    print(f"S3 not available: {e}")
    s3_client = None
    s3_resource = None
    S3_AVAILABLE = False

# S3 utility functions (same as before)
def is_s3_path(path: str) -> bool:
    """Check if the given path is an S3 path"""
    return path.startswith('s3://') or path.startswith('s3a://') or path.startswith('s3n://')

def parse_s3_path(s3_path: str) -> tuple:
    """Parse S3 path to extract bucket and key"""
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    elif s3_path.startswith('s3a://') or s3_path.startswith('s3n://'):
        s3_path = s3_path[6:]
    
    parts = s3_path.split('/', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        return parts[0], ""

def download_s3_file_to_temp(bucket: str, key: str) -> str:
    """Download S3 file to temporary local file and return the path"""
    try:
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=Path(key).suffix)
        temp_path = temp_file.name
        temp_file.close()
        
        # Download from S3
        s3_client.download_file(bucket, key, temp_path)
        return temp_path
    except Exception as e:
        raise Exception(f"Failed to download S3 file s3://{bucket}/{key}: {str(e)}")

def list_s3_objects(bucket: str, prefix: str = "") -> list:
    """List objects in S3 bucket with given prefix"""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        return []
    except Exception as e:
        raise Exception(f"Failed to list S3 objects in bucket {bucket}: {str(e)}")

# Initialize MCP server and in-memory DuckDB connection
try:
    mcp = FastMCP("file_query_mcp_secure")
    print("FastMCP server initialized for secure web deployment")
except Exception as e:
    print(f"Failed to initialize FastMCP: {e}")
    sys.exit(1)

try:
    con = duckdb.connect(database=':memory:')  # In-memory database for fast querying
    print("DuckDB connection established")
except Exception as e:
    print(f"Failed to connect to DuckDB: {e}")
    sys.exit(1)

# Pydantic models for type validation and structure
class FileSchemaOverride(BaseModel):  
    """Model for overriding column data types when loading files"""
    file_name: str
    schema_override_input: Dict[str,str]

class S3Config(BaseModel):
    """Model for AWS S3 configuration"""
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: Optional[str] = None

# MCP Tools (same as before, but now secured)
@mcp.tool()
def configure_s3_credentials(s3_config: S3Config) -> str:
    """Configure AWS S3 credentials for the MCP server"""
    global s3_client, s3_resource, S3_AVAILABLE
    
    # First check if S3 is already configured from environment variables
    if S3_AVAILABLE and s3_client is not None:
        return f"S3 is already configured from environment variables for bucket: {S3_BUCKET_NAME}, folder: {S3_FOLDER_PATH}, region: {S3_REGION}"
    
    try:
        # Create new S3 client with provided credentials
        if s3_config.aws_access_key_id and s3_config.aws_secret_access_key:
            session = boto3.Session(
                aws_access_key_id=s3_config.aws_access_key_id,
                aws_secret_access_key=s3_config.aws_secret_access_key,
                region_name=s3_config.aws_region or S3_REGION
            )
            s3_client = session.client('s3')
            s3_resource = session.resource('s3')
        else:
            # Use environment variables
            aws_access_key = get_env_with_fallbacks('AWS_ACCESS_KEY_ID')
            aws_secret_key = get_env_with_fallbacks('AWS_SECRET_ACCESS_KEY')
            
            if aws_access_key and aws_secret_key:
                s3_client = boto3.client('s3', 
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key,
                                       region_name=s3_config.aws_region or S3_REGION)
                s3_resource = boto3.resource('s3', 
                                           aws_access_key_id=aws_access_key,
                                           aws_secret_access_key=aws_secret_key,
                                           region_name=s3_config.aws_region or S3_REGION)
            else:
                # Use default credentials
                s3_client = boto3.client('s3', region_name=s3_config.aws_region or S3_REGION)
                s3_resource = boto3.resource('s3', region_name=s3_config.aws_region or S3_REGION)
        
        S3_AVAILABLE = True
        
        # Test connection
        s3_client.list_buckets()
        return f"Successfully configured S3 client for bucket: {S3_BUCKET_NAME}, folder: {S3_FOLDER_PATH}"
        
    except Exception as e:
        S3_AVAILABLE = False
        return f"Failed to configure S3 credentials: {str(e)}"

@mcp.tool()
def check_s3_status() -> str:
    """Check current S3 configuration status"""
    status_info = f"""
S3 Configuration Status:
- S3 Available: {S3_AVAILABLE}
- Bucket: {S3_BUCKET_NAME}
- Folder: {S3_FOLDER_PATH}  
- Region: {S3_REGION}
- Client Initialized: {s3_client is not None}

Environment Variables:
- AWS_ACCESS_KEY_ID: {'SET' if get_env_with_fallbacks('AWS_ACCESS_KEY_ID') else 'NOT SET'}
- AWS_SECRET_ACCESS_KEY: {'SET' if get_env_with_fallbacks('AWS_SECRET_ACCESS_KEY') else 'NOT SET'}
- AWS_DEFAULT_REGION: {get_env_with_fallbacks('AWS_DEFAULT_REGION', 'NOT SET')}

If S3 is available, you can directly use list_available_files() without configuring credentials.
"""
    return status_info

@mcp.tool()
def list_available_files() -> str:
    """List all available data files in the configured S3 bucket and folder"""
    if not S3_AVAILABLE:
        return "Error: S3 is not available. Please configure S3 credentials first."
    
    try:
        # List objects in the configured S3 folder
        objects = list_s3_objects(S3_BUCKET_NAME, S3_FOLDER_PATH)
        
        data_files = {}
        if os.path.exists("schema_descriptions.json"):
            with open("schema_descriptions.json", "w") as f:
                f.write("{}")  # Reset schema cache
        
        # Filter for supported file types
        for obj_key in objects:
            if obj_key.endswith(('.csv', '.json', '.xlsx', '.parquet')):
                file_name = os.path.basename(obj_key)
                s3_path = f"s3://{S3_BUCKET_NAME}/{obj_key}"
                
                # Create sanitized table name
                file_table_name = f"_{file_name.replace('.', '_').replace('-', '_')}"
                
                # Store file metadata
                data_files[file_name] = {
                    "path": s3_path,
                    "table_name": file_table_name,
                    "bucket": S3_BUCKET_NAME,
                    "key": obj_key,
                    "is_s3": True
                }
        
        # Handle case when no files are found
        if not data_files:
            json.dump({"error": f"No data files found in {S3_DATA_PATH}"}, open("data_files.json", "w"), indent=4)
            return f"No data files found in {S3_DATA_PATH}"
        else:
            # Save catalog to JSON file for persistence
            json.dump(data_files, open("data_files.json", "w"), indent=4)
        
        # Return user-friendly list of file names
        names_list = list(data_files.keys())
        all_file_names_str = "\n".join(names_list)
        return f"Available files in {S3_DATA_PATH}:\n{all_file_names_str}"
        
    except Exception as e:
        return f"Error listing files from {S3_DATA_PATH}: {str(e)}"

@mcp.tool()
def get_data_model_info() -> str:
    """Get comprehensive information about the Silver Layer data model"""
    return """
SILVER LAYER DATA MODEL - QUICK REFERENCE

CORE TABLES BY PURPOSE:

CLIENT ACTIVITY & ENGAGEMENT:
- fact_invoice.parquet: Use for client activity periods (last invoice dates)
- fact_monthly_summary.parquet: Use for period-specific revenue analysis
- dim_contact.parquet: Client master data (filter by is_current = 1)

REVENUE & FINANCIAL ANALYSIS:
- fact_monthly_summary.parquet: Pre-aggregated monthly revenue (PERIOD ANALYSIS)
- fact_invoice.parquet: Transaction-level data (ACTIVITY ANALYSIS)
- fact_financial_performance.parquet: Comprehensive financial metrics
- fact_executive_kpi.parquet: Executive KPIs and projections

RISK & COMPLIANCE:
- fact_risk_event.parquet: Risk events and assessments
- fact_anomaly.parquet: Anomaly detection results
- fact_payment_delay.parquet: Payment delay analysis
- fact_unpaid_invoice.parquet: Outstanding invoice analysis

STRATEGIC INSIGHTS:
- fact_strategic_insight.parquet: Strategic recommendations
- fact_action_recommendation.parquet: Action recommendations
- fact_rate_analysis.parquet: Billing rate analysis

RELATIONSHIPS:
- bridge_contact_project.parquet: Client-Project relationships
- dim_project.parquet: Project definitions
- dim_currency.parquet: Currency reference data
- dim_date.parquet: Date dimension for time analysis

CRITICAL BUSINESS RULES:

CORRECT PATTERNS:
1. Client Activity: Use fact_invoice for last activity dates
2. Period Revenue: Use fact_monthly_summary for 3m/6m/12m revenue
3. Cumulative Counts: 3m includes 6m, 6m includes 12m
4. Current Date: Use datetime.now().date() NOT parquet dates
5. Active Clients: Filter dim_contact by is_current = 1

ANTI-PATTERNS:
1. Don't use fact_invoice for period revenue (gives lifetime totals)
2. Don't use mutually exclusive periods (decreases client counts)
3. Don't ignore is_current filter on dim_contact
4. Don't use parquet file dates as current date

VALIDATION CHECKLIST:
- Client counts increase with longer periods
- Revenue amounts reasonable for time period
- Cross-check activity with revenue data
- Use consistent date references
- Filter for valid records

COMMON ANALYSIS PATTERNS:
- Client Engagement: fact_invoice + dim_contact
- Revenue Trends: fact_monthly_summary + LAG functions
- Risk Assessment: fact_risk_event + fact_financial_performance
- Churn Analysis: fact_invoice activity + fact_executive_kpi
"""

@mcp.tool()
def list_file_schema(file_names_list: list) -> str:
    """Analyze file schemas and load data into memory for querying"""
    # Side Effects:
    # - Loads data files into DuckDB memory
    # - Creates/updates 'schema_descriptions.json' cache
    # Load existing schema cache if available
    if os.path.exists("schema_descriptions.json"):
        with open("schema_descriptions.json", "r") as f:
            schema_descriptions = json.load(f)
    else:
        schema_descriptions = {}
    
    # Load file catalog
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    else:
        return "Error: No files catalog found. Please run list_available_files() first."
    
    # Process each requested file
    for file in file_names_list:
        # Check if file exists in catalog
        if file not in data_files.keys():
            schema_descriptions[file] = f"File {file} not found in {S3_DATA_PATH}.\n"
            continue
            
        path = data_files[file]["path"]
        table_name = data_files[file]['table_name']
        
        try:
            # Skip if schema already cached
            if file not in schema_descriptions.keys():
                # Handle S3 files by downloading to temp location
                actual_path = path
                temp_file_path = None
                
                if data_files[file].get("is_s3", False):
                    bucket = data_files[file]["bucket"]
                    key = data_files[file]["key"]
                    temp_file_path = download_s3_file_to_temp(bucket, key)
                    actual_path = temp_file_path
                
                # Load file based on extension using Polars
                if file.endswith('.csv'):
                    df = pl.read_csv(actual_path)
                    con.register(table_name, df)
                elif file.endswith('.json'):
                    df = pl.read_json(actual_path)
                    con.register(table_name, df)
                elif file.endswith('.xlsx'):
                    df = pl.read_excel(actual_path)
                    con.register(table_name, df)
                elif file.endswith('.parquet'):
                    df = pl.read_parquet(actual_path)
                    con.register(table_name, df)
                else:
                    schema_descriptions["file"] = f"Unsupported file format for {file}.\n"
                    continue
                
                # Clean up temporary file if it was created
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.unlink(temp_file_path)
                    except:
                        pass
                
                # Generate comprehensive schema description
                schema_descriptions[file] = f"""
                Schema for {file}:\n{df.schema}\n\n
                Descriptive statistics for {file}:\n{df.describe()}\n\n
                Top 5 rows of {file}:\n{df.head(5).to_pandas().to_string()}\n
                --- \n"""
                
                # Clean up memory
                del df
                gc.collect()
                
            # Update schema cache
            json.dump(schema_descriptions, open("schema_descriptions.json", "w"), indent=4)
            
        except Exception as e:
            # Handle file reading errors gracefully
            schema_descriptions[file] = f"Error reading {file}: {str(e)}\n"
            schema_descriptions[file] += f"S3 file: {data_files[file]['path']}\n---\n to help with schema inference and override"
            
    # Compile output for requested files
    output_string = "\n".join([schema_descriptions[f] for f in file_names_list if f in schema_descriptions.keys()])
    return output_string

@mcp.tool()
def load_override_schema(schema_json: FileSchemaOverride) -> str:
    """Load data file with custom column data types"""
    # Load file catalog and schema cache
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)   
    else:
        return "Error: No files catalog found. Please run list_available_files() first."
        
    if os.path.exists("schema_descriptions.json"):
        with open("schema_descriptions.json", "r") as f:
            schema_descriptions = json.load(f)
    else:
        schema_descriptions = {}
    
    # Validate file exists
    if schema_json.file_name not in data_files.keys():
        return f"Error: File {schema_json.file_name} not found in {S3_DATA_PATH}."
    
    path = data_files[schema_json.file_name]["path"]
    table_name = data_files[schema_json.file_name]['table_name']
    
    # Convert string type names to Polars data types
    schema_override = {}
    for col, dtype in schema_json.schema_override_input.items():
        if dtype.lower() == "int":
            schema_override[col] = pl.Int64
        elif dtype.lower() == "float":
            schema_override[col] = pl.Float64
        elif dtype.lower() == "str" or dtype.lower() == "string":
            schema_override[col] = pl.Utf8
        elif dtype.lower() == "bool":
            schema_override[col] = pl.Boolean
        elif dtype.lower() == "date":
            schema_override[col] = pl.Date
        elif dtype.lower() == "datetime":
            schema_override[col] = pl.Datetime
        else:
            return f"Error: Unsupported data type {dtype} for column {col}."
    
    try:
        # Handle S3 files by downloading to temp location
        actual_path = path
        temp_file_path = None
        
        if data_files[schema_json.file_name].get("is_s3", False):
            bucket = data_files[schema_json.file_name]["bucket"]
            key = data_files[schema_json.file_name]["key"]
            temp_file_path = download_s3_file_to_temp(bucket, key)
            actual_path = temp_file_path
        
        # Load file with custom schema based on file type
        if schema_json.file_name.endswith('.csv'):
            df = pl.read_csv(actual_path, dtypes=schema_override)
            con.register(table_name, df)
        elif schema_json.file_name.endswith('.json'):
            df = pl.read_json(actual_path, dtypes=schema_override)
            con.register(table_name, df)
        elif schema_json.file_name.endswith('.xlsx'):
            df = pl.read_excel(actual_path, dtypes=schema_override)
            con.register(table_name, df)
        elif schema_json.file_name.endswith('.parquet'):
            df = pl.read_parquet(actual_path, dtypes=schema_override)
            con.register(table_name, df)
        else:
            return f"Error: Unsupported file format for {schema_json.file_name}."
        
        # Clean up temporary file if it was created
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass
        
        # Update schema description with override information
        schema_descriptions[schema_json.file_name] = f"""
        Schema for {schema_json.file_name} with override:\n{df.schema}\n\n
        Descriptive statistics for {schema_json.file_name}:\n{df.describe()}\n\n
        Top 5 rows of {schema_json.file_name}:\n{df.head(5).to_pandas().to_string()}\n
        --- \n"""
        
        # Save updated schema cache
        json.dump(schema_descriptions, open("schema_descriptions.json", "w"), indent=4)
        
        # Clean up memory
        del df
        gc.collect()
        
        return f"""Successfully loaded {schema_json.file_name} with override schema.
        Here is the schema:\n{schema_descriptions[schema_json.file_name]}
        """
        
    except Exception as e:
        return f"Error loading {schema_json.file_name} with override schema: {str(e)}"

@mcp.tool()
def query_files(raw_query: str) -> str:
    """Execute SQL queries against loaded data files"""
    # Translates file names in SQL queries to their corresponding table names
    # and executes the query using DuckDB.
    words = raw_query.split()
    
    # Load file catalog for name translation
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    else:
        return "Error: No files catalog found. Please run list_available_files() first."
    
    # Replace file names and paths with table names in query
    for file_name in data_files.keys():
        # Replace direct file name references
        if file_name in words:
            raw_query = raw_query.replace(file_name, data_files[file_name]['table_name'])
            continue
            
        # Replace file path references
        path = data_files[file_name]["path"]
        if path in words:
            raw_query = raw_query.replace(f"{path}", data_files[file_name]['table_name'])
            continue

    query = raw_query
    
    try:
        # Execute query and return results as formatted string
        result_df = con.execute(query).df()
        result_str = result_df.to_string()
        return result_str
    except Exception as e:
        return f"Error executing query: {str(e)}"

# ============================================================================
# SECURE WEB SERVER SETUP FOR HTTP/SSE TRANSPORT
# ============================================================================

# Create FastAPI app for HTTP transport
app = FastAPI(
    title="Secure MCP File Query Server", 
    version="1.0.0",
    description="Secure MCP server with API key authentication"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST", "GET", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Static files for web UI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

# Serve static files from web_ui directory
if os.path.exists("web_ui"):
    app.mount("/static", StaticFiles(directory="web_ui"), name="static")

@app.get("/")
async def root():
    """Serve the web UI or return API info"""
    # Check if web UI files exist
    web_ui_path = "web_ui/index.html"
    if os.path.exists(web_ui_path):
        return FileResponse(web_ui_path)
    else:
        return {
            "status": "healthy",
            "server": "Secure MCP File Query Server",
            "version": "1.0.0",
            "authentication": "required",
            "web_ui": "not_found"
        }

@app.get("/web")
async def web_ui():
    """Direct access to web UI"""
    web_ui_path = "web_ui/index.html"
    if os.path.exists(web_ui_path):
        return FileResponse(web_ui_path)
    else:
        raise HTTPException(status_code=404, detail="Web UI not found")

@app.get("/health")
async def health_check(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """Detailed health check (requires authentication)"""
    
    return {
        "status": "healthy",
        "mcp_server": "initialized",
        "duckdb": "connected",
        "s3_available": S3_AVAILABLE,
        "environment": {
            "bucket": S3_BUCKET_NAME,
            "folder": S3_FOLDER_PATH,
            "region": S3_REGION,
            "credentials_configured": S3_AVAILABLE
        },
        "security": {
            "authentication": "enabled"
        }
    }

@app.post("/mcp")
async def mcp_endpoint(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """Secure MCP protocol endpoint for direct HTTP communication"""
    
    try:
        # Parse MCP message
        body = await request.json()
        
        # Handle different MCP methods
        method = body.get("method")
        params = body.get("params", {})
        request_id = body.get("id")
        
        response = {"jsonrpc": "2.0", "id": request_id}
        
        if method == "initialize":
            response["result"] = {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "file_query_mcp_secure",
                    "version": "1.0.0"
                }
            }
        
        elif method == "tools/list":
            # Return list of available tools with proper inputSchema
            tools = [
                {
                    "name": "check_s3_status",
                    "description": "Check current S3 configuration status",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                },
                {
                    "name": "configure_s3_credentials",
                    "description": "Configure AWS S3 credentials for the MCP server",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "aws_access_key_id": {
                                "type": "string",
                                "description": "AWS access key ID"
                            },
                            "aws_secret_access_key": {
                                "type": "string", 
                                "description": "AWS secret access key"
                            },
                            "aws_region": {
                                "type": "string",
                                "description": "AWS region"
                            }
                        }
                    }
                },
                {
                    "name": "list_available_files", 
                    "description": "List all available data files in the configured S3 bucket and folder",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                },
                {
                    "name": "get_data_model_info",
                    "description": "Get comprehensive information about the Silver Layer data model",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                },
                {
                    "name": "list_file_schema",
                    "description": "Analyze file schemas and load data into memory for querying",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "file_names_list": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                },
                                "description": "List of file names to analyze"
                            }
                        },
                        "required": ["file_names_list"]
                    }
                },
                {
                    "name": "load_override_schema",
                    "description": "Load data file with custom column data types",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "file_name": {
                                "type": "string",
                                "description": "Name of the file to apply schema override"
                            },
                            "schema_override_input": {
                                "type": "object",
                                "description": "Dictionary mapping column names to data types",
                                "additionalProperties": {
                                    "type": "string"
                                }
                            }
                        },
                        "required": ["file_name", "schema_override_input"]
                    }
                },
                {
                    "name": "query_files",
                    "description": "Execute SQL queries against loaded data files",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "raw_query": {
                                "type": "string",
                                "description": "SQL query string where file names (with extensions) are used as table names"
                            }
                        },
                        "required": ["raw_query"]
                    }
                }
            ]
            
            response["result"] = {"tools": tools}
        
        elif method == "tools/call":
            # Call the specified tool
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            # Map tool names to functions
            tool_functions = {
                "check_s3_status": check_s3_status,
                "configure_s3_credentials": configure_s3_credentials,
                "list_available_files": list_available_files,
                "get_data_model_info": get_data_model_info,
                "list_file_schema": list_file_schema,
                "load_override_schema": load_override_schema,
                "query_files": query_files
            }
            
            if tool_name in tool_functions:
                try:
                    # Call the function with arguments
                    if arguments:
                        # Handle different argument types
                        if tool_name == "configure_s3_credentials":
                            result = tool_functions[tool_name](S3Config(**arguments))
                        else:
                            result = tool_functions[tool_name](**arguments)
                    else:
                        result = tool_functions[tool_name]()
                    
                    response["result"] = {"content": [{"type": "text", "text": result}]}
                except Exception as e:
                    response["error"] = {"code": -1, "message": str(e)}
            else:
                response["error"] = {"code": -2, "message": f"Tool {tool_name} not found"}
        
        else:
            response["error"] = {"code": -3, "message": f"Unknown method: {method}"}
        
        return response
        
    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -4, "message": f"Server error: {str(e)}"}
        }

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    api_key: str = Depends(verify_api_key)
):
    """Upload a file to be processed by the MCP server"""
    try:
        # Validate file type
        allowed_extensions = ['.csv', '.json', '.xlsx', '.parquet']
        file_extension = Path(file.filename).suffix.lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"File type {file_extension} not supported. Allowed types: {', '.join(allowed_extensions)}"
            )
        
        # Save file to temporary location
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_extension)
        temp_path = temp_file.name
        temp_file.close()
        
        # Write uploaded file to temp location
        with open(temp_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Process the file using MCP tools
        try:
            # Load file into DuckDB
            table_name = f"uploaded_{file.filename.replace('.', '_').replace('-', '_')}"
            
            if file_extension == '.csv':
                df = pl.read_csv(temp_path)
            elif file_extension == '.json':
                df = pl.read_json(temp_path)
            elif file_extension == '.xlsx':
                df = pl.read_excel(temp_path)
            elif file_extension == '.parquet':
                df = pl.read_parquet(temp_path)
            
            con.register(table_name, df)
            
            # Generate schema description
            schema_info = f"""
            File uploaded successfully: {file.filename}
            Schema: {df.schema}
            Rows: {len(df)}
            Columns: {len(df.columns)}
            
            Descriptive statistics:
            {df.describe()}
            
            Top 5 rows:
            {df.head(5).to_pandas().to_string()}
            """
            
            # Clean up temp file
            os.unlink(temp_path)
            
            return {
                "status": "success",
                "filename": file.filename,
                "table_name": table_name,
                "schema_info": schema_info,
                "message": f"File {file.filename} uploaded and loaded successfully. You can now query it using the table name: {table_name}"
            }
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/api/tools")
async def get_available_tools(api_key: str = Depends(verify_api_key)):
    """Get list of available MCP tools"""
    tools = [
        {
            "name": "check_s3_status",
            "description": "Check current S3 configuration status",
            "category": "Configuration"
        },
        {
            "name": "configure_s3_credentials", 
            "description": "Configure AWS S3 credentials for the MCP server",
            "category": "Configuration"
        },
        {
            "name": "list_available_files",
            "description": "List all available data files in the configured S3 bucket and folder",
            "category": "File Management"
        },
        {
            "name": "get_data_model_info",
            "description": "Get comprehensive information about the Silver Layer data model",
            "category": "Information"
        },
        {
            "name": "list_file_schema",
            "description": "Analyze file schemas and load data into memory for querying",
            "category": "Analysis"
        },
        {
            "name": "load_override_schema",
            "description": "Load data file with custom column data types",
            "category": "Analysis"
        },
        {
            "name": "query_files",
            "description": "Execute SQL queries against loaded data files",
            "category": "Query"
        }
    ]
    
    return {"tools": tools}

@app.post("/api/chat")
async def chat_endpoint(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """Simplified chat endpoint for web UI"""
    try:
        body = await request.json()
        message = body.get("message", "").strip()
        
        if not message:
            raise HTTPException(status_code=400, detail="Message is required")
        
        # Determine tool call based on message
        tool_call = determine_tool_from_message(message)
        
        # Call the MCP tool
        result = await call_mcp_tool(tool_call)
        
        return {
            "response": result,
            "tool_used": tool_call["name"],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sse")
async def sse_endpoint(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """Server-Sent Events endpoint for MCP communication"""
    
    async def event_generator():
        try:
            # Read the request body
            body = await request.body()
            
            # For SSE transport, we need to handle MCP protocol messages
            # This is a simplified implementation - you might need to adjust based on your MCP client
            
            # Send initial connection message
            yield f"data: {json.dumps({'type': 'connection', 'status': 'connected'})}\n\n"
            
            # Handle MCP protocol here
            # This would typically involve parsing MCP messages and routing to appropriate tools
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

# Helper functions for chat endpoint
def determine_tool_from_message(message: str) -> dict:
    """Determine which MCP tool to call based on message content"""
    lower_message = message.lower()
    
    if 'list' in lower_message and ('file' in lower_message or 'available' in lower_message):
        return {
            "name": "list_available_files",
            "arguments": {}
        }
    elif 's3' in lower_message and 'status' in lower_message:
        return {
            "name": "check_s3_status", 
            "arguments": {}
        }
    elif 'data model' in lower_message or 'model info' in lower_message:
        return {
            "name": "get_data_model_info",
            "arguments": {}
        }
    elif 'schema' in lower_message and 'analyze' in lower_message:
        # Extract file names from message
        file_names = extract_file_names_from_message(message)
        return {
            "name": "list_file_schema",
            "arguments": {
                "file_names_list": file_names
            }
        }
    elif 'query' in lower_message or 'sql' in lower_message:
        # Extract SQL query from message
        query = extract_sql_query_from_message(message)
        return {
            "name": "query_files",
            "arguments": {
                "raw_query": query
            }
        }
    else:
        # Default to listing files for general queries
        return {
            "name": "list_available_files",
            "arguments": {}
        }

def extract_file_names_from_message(message: str) -> list:
    """Extract file names from message"""
    file_extensions = ['.csv', '.json', '.xlsx', '.parquet']
    words = message.split()
    file_names = []
    
    for word in words:
        for ext in file_extensions:
            if ext in word.lower():
                file_names.append(word)
                break
    
    return file_names

def extract_sql_query_from_message(message: str) -> str:
    """Extract SQL query from message"""
    sql_keywords = ['select', 'from', 'where', 'group by', 'order by', 'limit']
    lines = message.split('\n')
    
    for line in lines:
        lower_line = line.lower().strip()
        if any(keyword in lower_line for keyword in sql_keywords):
            return line.strip()
    
    return message  # Return the whole message if no clear SQL found

async def call_mcp_tool(tool_call: dict) -> str:
    """Call an MCP tool and return the result"""
    tool_name = tool_call["name"]
    arguments = tool_call["arguments"]
    
    # Map tool names to functions
    tool_functions = {
        "check_s3_status": check_s3_status,
        "configure_s3_credentials": configure_s3_credentials,
        "list_available_files": list_available_files,
        "get_data_model_info": get_data_model_info,
        "list_file_schema": list_file_schema,
        "load_override_schema": load_override_schema,
        "query_files": query_files
    }
    
    if tool_name in tool_functions:
        try:
            if arguments:
                result = tool_functions[tool_name](**arguments)
            else:
                result = tool_functions[tool_name]()
            return result
        except Exception as e:
            return f"Error calling {tool_name}: {str(e)}"
    else:
        return f"Tool {tool_name} not found"

if __name__ == "__main__":
    # Get port from environment (Render.com sets PORT)
    port = int(os.getenv("PORT", 8000))
    
    print(f"Starting SECURE MCP Web Server for S3 Bucket: {S3_BUCKET_NAME}")
    print(f"Data Folder: {S3_FOLDER_PATH}")
    print(f"Region: {S3_REGION}")
    print(f"S3 Available: {S3_AVAILABLE}")
    print(f"Security: API Key Authentication {'ENABLED' if API_KEY != 'your-secure-api-key-here' else 'DISABLED (SECURITY RISK!)'}")
    print("=" * 50)
    print(f"Server starting on port {port}...")
    
    # Start the web server
    uvicorn.run(app, host="0.0.0.0", port=port)
