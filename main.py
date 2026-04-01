import os
import secrets
import struct
import hashlib
import time

import pyodbc
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from msal import ConfidentialClientApplication
from pydantic import BaseModel

app = FastAPI(title="Pine Fabric Connector")

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# Session store: token -> {user, expires}
_sessions: dict[str, dict] = {}


def _get_users() -> dict[str, dict]:
    """Parse USERS env var: 'user1:pass1:role,user2:pass2:role' -> {user: {password, role}}"""
    raw = os.environ.get("USERS", "")
    if not raw:
        return {}
    users = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            parts = pair.split(":")
            u = parts[0].strip()
            p = parts[1].strip() if len(parts) > 1 else ""
            role = parts[2].strip() if len(parts) > 2 else "admin"
            users[u] = {"password": p, "role": role}
    return users


class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/auth/login")
def login(req: LoginRequest):
    users = _get_users()
    if not users:
        raise HTTPException(status_code=500, detail="No users configured")
    user_data = users.get(req.username)
    if not user_data or not secrets.compare_digest(user_data["password"], req.password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = secrets.token_hex(32)
    _sessions[token] = {"user": req.username, "role": user_data["role"], "expires": time.time() + 86400}
    return {"token": token, "user": req.username, "role": user_data["role"]}


def _verify_auth(request: Request, api_key: str = Security(API_KEY_HEADER)):
    # Check API key first (for programmatic access)
    expected_key = os.environ.get("API_KEY", "")
    if api_key and expected_key and secrets.compare_digest(api_key, expected_key):
        return
    # Check session token (for dashboard users)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        session = _sessions.get(token)
        if session and session["expires"] > time.time():
            return
    raise HTTPException(status_code=401, detail="Invalid or missing credentials")

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


@app.get("/api/tables", dependencies=[Depends(_verify_auth)])
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


@app.get("/api/contracts/master", dependencies=[Depends(_verify_auth)])
def contracts_master():
    return _query_table("contracts_master")


@app.get("/api/contracts/alerts", dependencies=[Depends(_verify_auth)])
def contracts_alerts():
    return _query_table("contracts_alerts")


@app.get("/api/contracts/cpi", dependencies=[Depends(_verify_auth)])
def contracts_cpi():
    return _query_table("contracts_cpi")


@app.get("/api/contracts/fee-analysis", dependencies=[Depends(_verify_auth)])
def contracts_fee_analysis():
    return _query_table("contracts_fee_analysis")


@app.get("/api/contracts/client-summary", dependencies=[Depends(_verify_auth)])
def contracts_client_summary():
    return _query_table("contracts_client_summary")


@app.get("/api/contracts/biz-line-summary", dependencies=[Depends(_verify_auth)])
def contracts_biz_line_summary():
    return _query_table("contracts_biz_line_summary")


@app.get("/api/contracts/data-quality", dependencies=[Depends(_verify_auth)])
def contracts_data_quality():
    return _query_table("contracts_data_quality")
