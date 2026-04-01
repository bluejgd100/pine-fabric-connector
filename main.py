import os
import struct

import pyodbc
from fastapi import FastAPI, HTTPException
from msal import ConfidentialClientApplication

app = FastAPI(title="Pine Fabric Connector")

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


@app.get("/health")
def health():
    cfg = _get_config()
    configured = all(cfg.values())
    env_keys = [k for k, v in cfg.items() if not v]
    return {"status": "ok", "configured": configured, "missing": env_keys}


@app.get("/api/tables")
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


@app.get("/api/contracts/master")
def contracts_master():
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.contracts_master")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return {"count": len(rows), "columns": columns, "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
