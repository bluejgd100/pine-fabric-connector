import os
import secrets
import struct

import pyodbc
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from msal import ConfidentialClientApplication

app = FastAPI(title="Pine Fabric Connector")

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    expected = os.environ.get("API_KEY", "")
    if not expected or not api_key or not secrets.compare_digest(api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

SCOPES = ["https://database.windows.net/.default"]
SQL_COPT_SS_ACCESS_TOKEN = 1256


def _get_config():
    return {
        "server": os.environ.get("FABRIC_SQL_SERVER", ""),
        "database": os.environ.get("FABRIC_DATABASE", ""),
        "tenant_id": os.environ.get("AZURE_TENANT_ID", ""),
        "client_id": os.environ.get("AZURE_CLIENT_ID", ""),
        "client_secret": os.environ.get("AZURE_CLIENT_SECRET", ""),
    }


def _get_access_token() -> str:
    cfg = _get_config()
    msal_app = ConfidentialClientApplication(
        cfg["client_id"],
        authority=f"https://login.microsoftonline.com/{cfg['tenant_id']}",
        client_credential=cfg["client_secret"],
    )
    result = msal_app.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL token error: {result.get('error_description', result)}")
    return result["access_token"]


def _get_connection() -> pyodbc.Connection:
    cfg = _get_config()
    token = _get_access_token()
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


@app.get("/")
def root():
    return FileResponse("frontend/index.html")


@app.get("/health")
def health():
    cfg = _get_config()
    configured = all(cfg.values())
    env_keys = [k for k, v in cfg.items() if not v]
    return {"status": "ok", "configured": configured, "missing": env_keys}


@app.get("/api/tables", dependencies=[Depends(_verify_api_key)])
def list_tables():
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'dbo' ORDER BY TABLE_NAME"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _query_table(table: str):
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM dbo.{table}")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return {"count": len(rows), "columns": columns, "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/contracts/master", dependencies=[Depends(_verify_api_key)])
def contracts_master():
    return _query_table("contracts_master")


@app.get("/api/contracts/alerts", dependencies=[Depends(_verify_api_key)])
def contracts_alerts():
    return _query_table("contracts_alerts")


@app.get("/api/contracts/cpi", dependencies=[Depends(_verify_api_key)])
def contracts_cpi():
    return _query_table("contracts_cpi")


@app.get("/api/contracts/fee-analysis", dependencies=[Depends(_verify_api_key)])
def contracts_fee_analysis():
    return _query_table("contracts_fee_analysis")


@app.get("/api/contracts/client-summary", dependencies=[Depends(_verify_api_key)])
def contracts_client_summary():
    return _query_table("contracts_client_summary")


@app.get("/api/contracts/biz-line-summary", dependencies=[Depends(_verify_api_key)])
def contracts_biz_line_summary():
    return _query_table("contracts_biz_line_summary")


@app.get("/api/contracts/data-quality", dependencies=[Depends(_verify_api_key)])
def contracts_data_quality():
    return _query_table("contracts_data_quality")
