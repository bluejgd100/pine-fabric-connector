import os
import struct

import pyodbc
from fastapi import FastAPI, HTTPException
from msal import ConfidentialClientApplication

app = FastAPI(title="Pine Fabric Connector")

FABRIC_SQL_SERVER = os.environ["FABRIC_SQL_SERVER"]
FABRIC_DATABASE = os.environ["FABRIC_DATABASE"]
AZURE_TENANT_ID = os.environ["AZURE_TENANT_ID"]
AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
AZURE_CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

SCOPES = ["https://database.windows.net/.default"]
SQL_COPT_SS_ACCESS_TOKEN = 1256


def _get_access_token() -> str:
    msal_app = ConfidentialClientApplication(
        AZURE_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{AZURE_TENANT_ID}",
        client_credential=AZURE_CLIENT_SECRET,
    )
    result = msal_app.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL token error: {result.get('error_description', result)}")
    return result["access_token"]


def _get_connection() -> pyodbc.Connection:
    token = _get_access_token()
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={FABRIC_SQL_SERVER};"
        f"DATABASE={FABRIC_DATABASE};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


@app.get("/health")
def health():
    return {"status": "ok"}


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
