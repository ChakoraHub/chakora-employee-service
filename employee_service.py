"""local
employee_service.py  ─  Employee Module FastAPI Microservice
Port : 8002
Run  : uvicorn employee_service:app --host 0.0.0.0 --port 8002

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Redis Cache Strategy (via redis_service on port 6380)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DB 0   session:{employee_id}               7 days   /session/set
  DB 1   user:{employee_id}                  30 min   /profile/set
  DB 2   roles:{employee_id}                 1 hr     /auth/set + /redis/set
  DB 11  employee:{employee_id}              15 min   personal details
  DB 11  salary:{employee_id}                2 min   ⚠ SHORT TTL — sensitive
  DB 11  leave:{employee_id}                 5 min    leave tracker
  DB 11  emp_service:appraisal_*             5 min
  DB 11  emp_service:hierarchy_*             5 min
  DB 11  emp_service:employee_queries        1 min    short — user writes invalidate
  DB 11  emp_service:festivals               5 min
  DB 11  emp_service:manager_pending_leaves  1 min
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
import json
from datetime import date, datetime
import boto3
import os
import oracledb
import pathlib
import traceback
import re
import urllib.parse
import urllib.request
import urllib.error
import asyncio
import hmac
import hashlib
import queue
import threading
import time
from dotenv import load_dotenv

try:
    from werkzeug.security import check_password_hash
except Exception:
    check_password_hash = None


class _OracleDictCursorMarker:
    pass


DictCursor = _OracleDictCursorMarker

# Load service-local env file so PM2/systemd runs pick up Redis settings.
load_dotenv(dotenv_path=pathlib.Path(__file__).parent / ".env", override=False)


# ══════════════════════════════════════════════════════════════
# REDIS CONFIGURATION
# ══════════════════════════════════════════════════════════════

def _clean_env_value(raw_value: Optional[str]) -> str:
    if raw_value is None:
        return ""
    value = str(raw_value).strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def _get_env_value(name: str, default: str = "") -> str:
    value = _clean_env_value(os.getenv(name))
    return value or default


REDIS_SERVICE_URL = _get_env_value("REDIS_SERVICE_URL", "http://127.0.0.1:6379")

# Shared Redis service DB allocation (mirrors redis_service.py constants)
DB_SESSIONS   = 0
DB_PROFILES   = 1
DB_AUTH       = 2
DB_FREQ_DATA  = 3
DB_RATE_LIMIT = 4
DB_API_CACHE  = 5   # student_service / home_service / generic cache
DB_EMPLOYEE   = 11  # Employee-only cache — isolated from DB 5 (no key collisions)

# ─────────────────────────────────────────────────────────────
# Employee-specific TTLs
# ─────────────────────────────────────────────────────────────
CACHE_TTL_SALARY   = 120    #  2 min  — sensitive, changes with payroll
CACHE_TTL_SHORT    = 60     #  1 min  — queries, pending leaves (user-write-heavy)
CACHE_TTL_MEDIUM   = 300    #  5 min  — most employee API responses
CACHE_TTL_PROFILE  = 900    # 15 min  — employee:{id} personal details
CACHE_TTL_ROLE     = 3_600  #  1 hr   — auth/roles (mirrors redis_service TTL_AUTH)
CACHE_TTL_SESSION  = 604_800  # 7 days — mirrors redis_service TTL_SESSION
CACHE_TTL_DAY      = 86_400   # 24 hrs — static data: festivals, birthdays (never change mid-day)


# ══════════════════════════════════════════════════════════════
# REDIS HTTP HELPERS
# ══════════════════════════════════════════════════════════════

def _redis_request(
    method: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    query: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Low-level HTTP call to redis_service. Never raises — returns success:False on error."""
    base = REDIS_SERVICE_URL.rstrip("/")
    query_params = {k: v for k, v in (query or {}).items() if v is not None}
    query_string = urllib.parse.urlencode(query_params)
    url = f"{base}{path}"
    if query_string:
        url = f"{url}?{query_string}"

    body = None
    headers: Dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=body, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=0.3) as resp:   # ← was 3s — now 300ms
            raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {"success": True}
    except urllib.error.HTTPError as exc:
        try:
            message = exc.read().decode("utf-8")
        except Exception:
            message = str(exc)
        return {"success": False, "message": message}
    except Exception as exc:
        return {"success": False, "message": str(exc)}


# ── Generic DB-5 (API cache) helpers ──────────────────────────

def redis_get_safe(key: str, db: int = DB_EMPLOYEE) -> Optional[str]:
    """GET from the specified Redis DB. Returns raw string or None."""
    response = _redis_request("GET", "/redis/get", query={"key": key, "db": db})
    if response.get("success") and response.get("found"):
        value = response.get("value")
        if isinstance(value, str):
            return value
        if value is None:
            return None
        return json.dumps(value, default=str)
    if not response.get("success"):
        print(f"⚠️  Redis GET failed for {key!r}: {response.get('message')}")
    return None


def redis_setex_safe(key: str, ttl: int, value: str, db: int = DB_EMPLOYEE) -> bool:
    """SETEX to the specified Redis DB."""
    response = _redis_request(
        "POST", "/redis/set",
        payload={"key": key, "value": value, "ttl": ttl, "db": db},
    )
    if not response.get("success"):
        print(f"⚠️  Redis SETEX failed for {key!r}: {response.get('message')}")
        return False
    return True


def redis_delete_safe(key: str, db: int = DB_EMPLOYEE) -> bool:
    """DEL from the specified Redis DB."""
    response = _redis_request(
        "POST", "/redis/delete",
        payload={"keys": [key], "db": db},
    )
    if not response.get("success"):
        print(f"⚠️  Redis DELETE failed for {key!r}: {response.get('message')}")
        return False
    return True


def redis_json_get_safe(key: str, db: int = DB_EMPLOYEE) -> Optional[Any]:
    """GET + JSON-decode. Returns None on miss or decode error."""
    cached = redis_get_safe(key, db=db)
    if not cached:
        return None
    try:
        return json.loads(cached)
    except Exception:
        return None


def redis_json_setex_safe(key: str, ttl: int, data: Any, db: int = DB_EMPLOYEE) -> bool:
    """JSON-encode + SETEX."""
    try:
        return redis_setex_safe(key, ttl, json.dumps(data, default=str), db=db)
    except Exception:
        return False


def cache_key(namespace: str, *parts: Any) -> str:
    """Build a namespaced cache key: emp_service:{namespace}:{PART1}:{PART2}:..."""
    suffix = ":".join(str(p).strip().upper() for p in parts)
    return f"emp_service:{namespace}:{suffix}" if suffix else f"emp_service:{namespace}"


# ── Canonical high-level cache helpers ────────────────────────

def cache_session(employee_id: str, data: Dict[str, Any]) -> None:
    """Store employee session in DB 0 under deterministic key session:{employee_id}."""
    _redis_request(
        "POST", "/redis/set",
        payload={
            "key": f"session:{employee_id}",
            "value": json.dumps(data, default=str),
            "ttl": CACHE_TTL_SESSION,
            "db": DB_SESSIONS,
        },
    )


def cache_user_profile(employee_id: str, data: Dict[str, Any]) -> None:
    """
    Store employee profile in DB 1 under key user:{employee_id}.
    Also stores under the canonical employee:{employee_id} key in DB 5
    for fast partial API cache lookups.
    """
    # DB 1 — canonical profile store
    _redis_request(
        "POST", "/redis/set",
        payload={
            "key": f"user:{employee_id}",
            "value": json.dumps(data, default=str),
            "ttl": CACHE_TTL_ROLE,   # 1 hr — profiles don't change often
            "db": DB_PROFILES,
        },
    )
    # DB 5 — fast API cache key employee:{employee_id}
    redis_json_setex_safe(f"employee:{employee_id}", CACHE_TTL_PROFILE, data)


def cache_authorization_roles(employee_id: str, is_manager_flag: bool) -> None:
    """Store role/auth data in DB 2 under key roles:{employee_id}."""
    auth_key = f"roles:{employee_id}"
    _redis_request(
        "POST", "/redis/set",
        payload={
            "key": auth_key,
            "value": json.dumps({"is_manager": bool(is_manager_flag)}),
            "ttl": CACHE_TTL_ROLE,
            "db": DB_AUTH,
        },
    )


def get_cached_authorization_roles(employee_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve role/auth data from DB 2."""
    auth_key = f"roles:{employee_id}"
    response = _redis_request("GET", "/redis/get", query={"key": auth_key, "db": DB_AUTH})
    if not response.get("success") or not response.get("found"):
        return None
    raw_value = response.get("value")
    try:
        return json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except Exception:
        return None


def cache_salary(employee_id: str, data: Any) -> None:
    """
    Store salary data in DB 5 under key salary:{employee_id}.
    Uses CACHE_TTL_SALARY (2 min) — short because salary is sensitive
    and changes with payroll runs.
    """
    redis_json_setex_safe(f"salary:{employee_id}", CACHE_TTL_SALARY, data)


def get_cached_salary(employee_id: str) -> Optional[Any]:
    return redis_json_get_safe(f"salary:{employee_id}")


def cache_leave(employee_id: str, data: Any) -> None:
    """Store leave-tracker data in DB 5 under key leave:{employee_id}."""
    redis_json_setex_safe(f"leave:{employee_id}", CACHE_TTL_MEDIUM, data)


def get_cached_leave(employee_id: str) -> Optional[Any]:
    return redis_json_get_safe(f"leave:{employee_id}")


def invalidate_leave(employee_id: str, manager_id: Optional[str] = None) -> None:
    """Bust all leave-related caches after an insert/update."""
    redis_delete_safe(f"leave:{employee_id}")
    redis_delete_safe(cache_key("leave_history", employee_id))
    if manager_id:
        redis_delete_safe(cache_key("manager_pending_leaves", manager_id))


# ── Rate-limit helper ──────────────────────────────────────────

def get_client_identifier(request: Request, fallback: str) -> str:
    forwarded_for = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    real_ip = (request.headers.get("x-real-ip") or "").strip()
    client_host = request.client.host if request.client else ""
    identifier = forwarded_for or real_ip or client_host or fallback
    return identifier.strip().upper() or fallback.strip().upper()


def redis_rate_limit_allow(key: str, limit: int, window_seconds: int) -> bool:
    endpoint, _, identifier = key.partition(":")
    if not endpoint:
        endpoint = "employee"
    if not identifier:
        identifier = key
    response = _redis_request(
        "POST", "/ratelimit/check",
        query={
            "identifier": identifier,
            "endpoint": endpoint,
            "limit": limit,
            "window": window_seconds,
        },
    )
    # Fail-open — don't block traffic if redis_service is down
    if not response.get("success"):
        print(f"⚠️  Rate-limit check failed for {key!r}: {response.get('message')}")
        return True
    return bool(response.get("allowed", True))


# ══════════════════════════════════════════════════════════════
# DATABASE CONNECTION
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
# DATABASE CONNECTION — pooled
# ══════════════════════════════════════════════════════════════

ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")
ORACLE_SCHEMA = (os.getenv("ORACLE_SCHEMA", "CHAKORA") or "CHAKORA").strip().upper()

if not re.fullmatch(r"[A-Z][A-Z0-9_]*", ORACLE_SCHEMA):
    ORACLE_SCHEMA = "CHAKORA"


class _OracleCompatRow(dict):
    def __init__(self, keys: List[str], values: List[Any]):
        super().__init__(zip(keys, values))
        self._values = list(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


class _OracleCompatCursor:
    def __init__(self, raw_cursor):
        self._cursor = raw_cursor

    def _rewrite_sql(self, sql: str, params):
        rewritten = sql
        if isinstance(params, dict):
            rewritten = re.sub(r"%\((\w+)\)s", r":\1", rewritten)
        elif params is not None:
            bind_index = 0

            def _replace_positional(_match):
                nonlocal bind_index
                bind_index += 1
                return f":{bind_index}"

            rewritten = re.sub(r"%s", _replace_positional, rewritten)

        rewritten = re.sub(r"CURRENT_TIMESTAMP\s*\(\s*\)", "CURRENT_TIMESTAMP", rewritten, flags=re.I)
        rewritten = re.sub(r"\s+ILIKE\s+", " LIKE ", rewritten, flags=re.I)
        rewritten = re.sub(r"\s+LIMIT\s+(\d+)\s+OFFSET\s+(\d+)", r" OFFSET \2 ROWS FETCH NEXT \1 ROWS ONLY", rewritten, flags=re.I)
        rewritten = re.sub(r"\s+LIMIT\s+(:\d+|\d+)", r" FETCH FIRST \1 ROWS ONLY", rewritten, flags=re.I)
        return rewritten

    def execute(self, sql, params=None):
        rewritten = self._rewrite_sql(sql, params)
        if params is None:
            return self._cursor.execute(rewritten)
        return self._cursor.execute(rewritten, params)

    def executemany(self, sql, seq_of_params):
        rewritten = self._rewrite_sql(sql, seq_of_params[0] if seq_of_params else None)
        return self._cursor.executemany(rewritten, seq_of_params)

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        keys = [col[0] for col in (self._cursor.description or [])]
        return _OracleCompatRow(keys, list(row))

    def fetchall(self):
        rows = self._cursor.fetchall() or []
        keys = [col[0] for col in (self._cursor.description or [])]
        return [_OracleCompatRow(keys, list(row)) for row in rows]

    def close(self):
        return self._cursor.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _OracleCompatConnection:
    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self, *args, **kwargs):
        return _OracleCompatCursor(self._conn.cursor())

    def __getattr__(self, name):
        return getattr(self._conn, name)

# ── Connection pool ────────────────────────────────────────────
_EMP_POOL_SIZE          = 3
_EMP_KEEPALIVE_INTERVAL = 240   # ping every 4 min — warehouse suspends after 5 min idle

_emp_conn_pool:    queue.Queue  = queue.Queue(maxsize=_EMP_POOL_SIZE)
_emp_first_ready:  threading.Event = threading.Event()


def _emp_open_raw_connection():
    dsn = oracledb.makedsn(
        host=ORACLE_HOST,
        port=ORACLE_PORT,
        service_name=ORACLE_SERVICE_NAME,
    )
    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=dsn,
    )
    conn = _OracleCompatConnection(conn)
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {ORACLE_SCHEMA}")
        cur.execute("SELECT 1")   # wake warehouse
    finally:
        cur.close()
    return conn


def _emp_is_alive(conn) -> bool:
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return True
    except Exception:
        return False


def _emp_pool_get():
    while True:
        try:
            conn = _emp_conn_pool.get_nowait()
            if _emp_is_alive(conn):
                return conn
            try: conn.close()
            except Exception: pass
        except queue.Empty:
            break
    try:
        return _emp_open_raw_connection()
    except Exception as e:
        print(f"❌ [emp-pool] Failed to open connection: {e}")
        return None


def _emp_pool_return(conn) -> None:
    if conn is None:
        return
    try:
        _emp_conn_pool.put_nowait(conn)
    except queue.Full:
        try: conn.close()
        except Exception: pass


def _emp_pool_keepalive():
    while True:
        time.sleep(_EMP_KEEPALIVE_INTERVAL)
        checked = []
        while True:
            try:
                conn = _emp_conn_pool.get_nowait()
                if _emp_is_alive(conn):
                    checked.append(conn)
                else:
                    try: conn.close()
                    except Exception: pass
                    try: checked.append(_emp_open_raw_connection())
                    except Exception as e: print(f"⚠️ [emp-pool] keepalive reconnect failed: {e}")
            except queue.Empty:
                break
        for c in checked:
            try: _emp_conn_pool.put_nowait(c)
            except queue.Full:
                try: c.close()
                except Exception: pass
        print(f"🔁 [emp-pool] keepalive done — {len(checked)} connection(s) alive")


def _emp_pool_prefill():
    def _fill():
        for i in range(_EMP_POOL_SIZE):
            try:
                conn = _emp_open_raw_connection()
                _emp_conn_pool.put_nowait(conn)
                print(f"✅ [emp-pool] warm connection added ({i+1}/{_EMP_POOL_SIZE})")
                if i == 0:
                    _emp_first_ready.set()
            except Exception as e:
                print(f"⚠️ [emp-pool] prefill failed slot {i+1}: {e}")
                if i == 0:
                    _emp_first_ready.set()
        threading.Thread(target=_emp_pool_keepalive, daemon=True).start()
    threading.Thread(target=_fill, daemon=True, name="emp-pool-prefill").start()


def get_db_connection():
    """Borrow a warm connection from the pool. Return with _emp_pool_return()."""
    return _emp_pool_get()



# Asset Helper functions

def generate_asset_id(conn) -> str:
    """
    Generate unique asset ID starting with 'CH' (ChakoraHub)
    Format: CH-YYYYMMDD-NNNN
    Example: CH-20250120-0001
    """
    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        
        # Get today's date
        today = datetime.now().strftime('%Y%m%d')
        prefix = f"CH-{today}"
        
        # Find the highest sequence number for today
        cursor.execute(
            """
            SELECT ASSET_ID 
            FROM ASSETS 
            WHERE ASSET_ID LIKE %s 
            ORDER BY ASSET_ID DESC 
            LIMIT 1
            """,
            (f"{prefix}-%",)
        )
        
        result = cursor.fetchone()
        
        if result:
            # Extract sequence number and increment
            last_id = result.get('ASSET_ID') or result.get('asset_id')
            try:
                sequence = int(last_id.split('-')[-1])
                new_sequence = sequence + 1
            except:
                new_sequence = 1
        else:
            new_sequence = 1
        
        # Format: CH-YYYYMMDD-NNNN (4-digit sequence)
        asset_id = f"{prefix}-{new_sequence:04d}"
        
        return asset_id
    
    except Exception as e:
        print(f"❌ Error generating asset ID: {e}")
        # Fallback to UUID-based ID
        import uuid
        return f"CH-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:4].upper()}"
    
    finally:
        if cursor:
            cursor.close()

def validate_employee_id(conn, employee_id: str) -> tuple[bool, str]:
    """
    Validate that employee ID exists in EMP_NRM_EMPLOYEES table
    Returns (is_valid, employee_name)
    """
    if not employee_id or employee_id.strip() == "":
        return True, ""  # Empty is valid (unassigned asset)
    
    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        employee_id_norm = employee_id.strip().upper()
        
        cursor.execute(
            """
            SELECT EMPLOYEE_ID, EMPLOYEE_NAME 
            FROM EMP_NRM_EMPLOYEES 
            WHERE UPPER(TRIM(EMPLOYEE_ID)) = %s
              AND UPPER(TRIM(COALESCE(STATUS, ''))) = 'ACTIVE'
            """,
            (employee_id_norm,)
        )
        
        result = cursor.fetchone()
        
        if result:
            emp_name = result.get('EMPLOYEE_NAME') or result.get('employee_name')
            return True, emp_name
        else:
            return False, ""
    
    except Exception as e:
        print(f"❌ Error validating employee ID: {e}")
        return False, ""
    
    finally:
        if cursor:
            cursor.close()

def create_audit_log(conn, asset_id: str, action: str, actioned_by: str, remarks: str = ""):
    """
    Create audit log entry for asset actions
    """
    cursor = None
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO ASSET_AUDIT_LOG (ASSET_ID, ACTION, ACTIONED_BY, ACTION_DATE, REMARKS)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (asset_id, action, actioned_by, datetime.now(), remarks)
        )
        
        conn.commit()
        print(f"✅ Audit log created: {asset_id} - {action}")
    
    except Exception as e:
        print(f"⚠️ Audit log creation failed: {e}")
        # Don't fail the main transaction if audit fails
    
    finally:
        if cursor:
            cursor.close()


def resolve_vendor_id(conn, vendor_value: Optional[str]) -> Optional[int]:
    """Resolve vendor input to numeric vendor ID.

    Accepts either an existing numeric ID or a vendor name (e.g., "Apple").
    If name does not exist, tries to create it in ASSET_VENDORS and re-resolve.
    """
    if vendor_value is None:
        return None

    raw = str(vendor_value).strip()
    if not raw:
        return None

    if raw.isdigit():
        return int(raw)

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        vendor_name = raw
        vendor_name_upper = vendor_name.upper()

        lookup_queries = [
            "SELECT ID AS VENDOR_ID FROM ASSET_VENDORS WHERE UPPER(TRIM(VENDOR_NAME)) = %s LIMIT 1",
            "SELECT VENDOR_ID FROM ASSET_VENDORS WHERE UPPER(TRIM(VENDOR_NAME)) = %s LIMIT 1",
            "SELECT ID AS VENDOR_ID FROM ASSET_VENDORS WHERE UPPER(TRIM(NAME)) = %s LIMIT 1",
            "SELECT VENDOR_ID FROM ASSET_VENDORS WHERE UPPER(TRIM(NAME)) = %s LIMIT 1",
        ]

        for q in lookup_queries:
            try:
                cursor.execute(q, (vendor_name_upper,))
                row = cursor.fetchone()
            except Exception:
                row = None

            if row:
                for key in ("VENDOR_ID", "ID", "vendor_id", "id"):
                    value = row.get(key)
                    if value is not None and str(value).strip().isdigit():
                        return int(str(value).strip())

        insert_queries = [
            "INSERT INTO ASSET_VENDORS (VENDOR_NAME) VALUES (%s)",
            "INSERT INTO ASSET_VENDORS (NAME) VALUES (%s)",
        ]

        for q in insert_queries:
            try:
                cursor.execute(q, (vendor_name,))
                break
            except Exception:
                continue

        # Re-resolve after insert attempt.
        for q in lookup_queries:
            try:
                cursor.execute(q, (vendor_name_upper,))
                row = cursor.fetchone()
            except Exception:
                row = None

            if row:
                for key in ("VENDOR_ID", "ID", "vendor_id", "id"):
                    value = row.get(key)
                    if value is not None and str(value).strip().isdigit():
                        return int(str(value).strip())

        raise HTTPException(
            status_code=400,
            detail=f"Invalid vendor '{vendor_name}'. Provide a valid vendor ID or configured vendor name.",
        )

    finally:
        if cursor:
            cursor.close()


def resolve_model_id(conn, type_id: Optional[int], model_value: Optional[str]) -> Optional[int]:
    """Resolve model input to numeric model ID.

    Accepts either an existing numeric ID or a model name from UI input.
    If model name is not present, attempts to create it in model master and re-resolve.
    """
    if model_value is None:
        return None

    raw = str(model_value).strip()
    if not raw:
        return None

    if raw.isdigit():
        return int(raw)

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        model_name = raw
        model_name_upper = model_name.upper()

        lookup_queries = [
            (
                "SELECT ID AS MODEL_ID FROM ASSET_MODELS WHERE UPPER(TRIM(MODEL_NAME)) = %s"
                " AND (%s IS NULL OR TYPE_ID = %s) ORDER BY ID DESC LIMIT 1",
                (model_name_upper, type_id, type_id),
            ),
            (
                "SELECT MODEL_ID FROM ASSET_MODELS WHERE UPPER(TRIM(MODEL_NAME)) = %s"
                " AND (%s IS NULL OR TYPE_ID = %s) ORDER BY MODEL_ID DESC LIMIT 1",
                (model_name_upper, type_id, type_id),
            ),
            (
                "SELECT ID AS MODEL_ID FROM ASSET_MODELS WHERE UPPER(TRIM(NAME)) = %s"
                " AND (%s IS NULL OR TYPE_ID = %s) ORDER BY ID DESC LIMIT 1",
                (model_name_upper, type_id, type_id),
            ),
            (
                "SELECT MODEL_ID FROM ASSET_MODELS WHERE UPPER(TRIM(NAME)) = %s"
                " AND (%s IS NULL OR TYPE_ID = %s) ORDER BY MODEL_ID DESC LIMIT 1",
                (model_name_upper, type_id, type_id),
            ),
        ]

        for q, params in lookup_queries:
            try:
                cursor.execute(q, params)
                row = cursor.fetchone()
            except Exception:
                row = None

            if row:
                for key in ("MODEL_ID", "ID", "model_id", "id"):
                    value = row.get(key)
                    if value is not None and str(value).strip().isdigit():
                        return int(str(value).strip())

        insert_queries = [
            ("INSERT INTO ASSET_MODELS (TYPE_ID, MODEL_NAME) VALUES (%s, %s)", (type_id, model_name)),
            ("INSERT INTO ASSET_MODELS (MODEL_NAME) VALUES (%s)", (model_name,)),
            ("INSERT INTO ASSET_MODELS (TYPE_ID, NAME) VALUES (%s, %s)", (type_id, model_name)),
            ("INSERT INTO ASSET_MODELS (NAME) VALUES (%s)", (model_name,)),
        ]

        for q, params in insert_queries:
            try:
                cursor.execute(q, params)
                break
            except Exception:
                continue

        for q, params in lookup_queries:
            try:
                cursor.execute(q, params)
                row = cursor.fetchone()
            except Exception:
                row = None

            if row:
                for key in ("MODEL_ID", "ID", "model_id", "id"):
                    value = row.get(key)
                    if value is not None and str(value).strip().isdigit():
                        return int(str(value).strip())

        raise HTTPException(
            status_code=400,
            detail=f"Invalid model '{model_name}'. Provide a valid model ID or configured model name.",
        )

    finally:
        if cursor:
            cursor.close()


def resolve_serial_id(conn, model_id: Optional[int], serial_value: Optional[str]) -> Optional[int]:
    """Resolve serial input to numeric serial ID and ensure serial master row exists.

    Works with ASSET_SERIALS / ASSETS_SERIALS style master tables containing
    columns ID, MODEL_ID, SERIAL_NO.
    """
    if serial_value is None:
        return None

    raw = str(serial_value).strip()
    if not raw:
        return None

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)

        if raw.isdigit():
            return int(raw)

        serial_no = raw
        serial_no_upper = serial_no.upper()

        lookup_queries = [
            (
                "SELECT ID FROM ASSET_SERIALS WHERE UPPER(TRIM(SERIAL_NO)) = %s"
                " AND (%s IS NULL OR MODEL_ID = %s) ORDER BY ID DESC LIMIT 1",
                (serial_no_upper, model_id, model_id),
            ),
            (
                "SELECT SERIAL_ID AS ID FROM ASSET_SERIALS WHERE UPPER(TRIM(SERIAL_NO)) = %s"
                " AND (%s IS NULL OR MODEL_ID = %s) ORDER BY SERIAL_ID DESC LIMIT 1",
                (serial_no_upper, model_id, model_id),
            ),
            (
                "SELECT ID FROM ASSETS_SERIALS WHERE UPPER(TRIM(SERIAL_NO)) = %s"
                " AND (%s IS NULL OR MODEL_ID = %s) ORDER BY ID DESC LIMIT 1",
                (serial_no_upper, model_id, model_id),
            ),
            (
                "SELECT SERIAL_ID AS ID FROM ASSETS_SERIALS WHERE UPPER(TRIM(SERIAL_NO)) = %s"
                " AND (%s IS NULL OR MODEL_ID = %s) ORDER BY SERIAL_ID DESC LIMIT 1",
                (serial_no_upper, model_id, model_id),
            ),
        ]

        for q, params in lookup_queries:
            try:
                cursor.execute(q, params)
                row = cursor.fetchone()
            except Exception:
                row = None

            if row and row.get("ID") is not None and str(row.get("ID")).strip().isdigit():
                return int(str(row.get("ID")).strip())

        insert_queries = [
            ("INSERT INTO ASSET_SERIALS (MODEL_ID, SERIAL_NO) VALUES (%s, %s)", (model_id, serial_no)),
            ("INSERT INTO ASSETS_SERIALS (MODEL_ID, SERIAL_NO) VALUES (%s, %s)", (model_id, serial_no)),
        ]

        for q, params in insert_queries:
            try:
                cursor.execute(q, params)
                break
            except Exception:
                continue

        for q, params in lookup_queries:
            try:
                cursor.execute(q, params)
                row = cursor.fetchone()
            except Exception:
                row = None

            if row and row.get("ID") is not None and str(row.get("ID")).strip().isdigit():
                return int(str(row.get("ID")).strip())

        raise HTTPException(
            status_code=400,
            detail=f"Invalid serial '{serial_no}'. Unable to resolve serial ID from serial master.",
        )

    finally:
        if cursor:
            cursor.close()

# ══════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ══════════════════════════════════════════════════════════════

class EmployeeLogin(BaseModel):
    employee_id: str
    password: str


class EmployeeLoginResponse(BaseModel):
    success: bool
    message: str
    employee_id: Optional[str] = None
    employee_db_id: Optional[str] = None   # e.g. 'APP005' — kept as str
    employee_name: Optional[str] = None
    employee_email: Optional[str] = None
    employee_department: Optional[str] = None
    employee_designation: Optional[str] = None
    profile_pic: Optional[str] = None
    is_manager: bool = False

class AssetRegisterRequest(BaseModel):
    """Asset registration/update request model"""
    
    # Required fields
    OWNED_BY: str = Field(..., description="Individual or Organization")
    TYPE_ID: int = Field(..., ge=1, description="Asset type ID (1-8)")
    MODEL_ID: str = Field(..., min_length=1, description="Model name")
    CONDITION: str = Field(..., description="New, Good, Fair, Poor")
    STATUS: str = Field(..., description="Active, Inactive, In Repair, Retired")
    
    # Optional fields
    ASSET_ID: Optional[str] = Field(None, description="For updates only")
    SERIAL_ID: Optional[str] = None
    VENDOR_ID: Optional[str] = None
    OS: Optional[str] = None
    RAM: Optional[str] = None
    SSD: Optional[str] = None
    ASSIGNED_TO: Optional[str] = None  # Employee ID or name
    LOCATION: Optional[str] = None
    PURCHASE_DATE: Optional[str] = None  # YYYY-MM-DD format
    WARRANTY_EXPIRY: Optional[str] = None
    PRICE: Optional[float] = Field(None, ge=0)
    NOTES: Optional[str] = None
    
    @validator('OWNED_BY')
    def validate_owned_by(cls, v):
        if v not in ['Individual', 'Organization']:
            raise ValueError('OWNED_BY must be Individual or Organization')
        return v
    
    @validator('TYPE_ID')
    def validate_type_id(cls, v):
        if v < 1 or v > 8:
            raise ValueError('TYPE_ID must be between 1 and 8')
        return v
    
    @validator('CONDITION')
    def validate_condition(cls, v):
        allowed = ['New', 'Good', 'Fair', 'Poor']
        if v not in allowed:
            raise ValueError(f'CONDITION must be one of: {", ".join(allowed)}')
        return v
    
    @validator('STATUS')
    def validate_status(cls, v):
        allowed = ['Active', 'Inactive', 'In Repair', 'Retired']
        if v not in allowed:
            raise ValueError(f'STATUS must be one of: {", ".join(allowed)}')
        return v
    
    @validator('PURCHASE_DATE')
    def validate_purchase_date(cls, v):
        if v:
            try:
                purchase_date = datetime.strptime(v, '%Y-%m-%d').date()
                if purchase_date > date.today():
                    raise ValueError('PURCHASE_DATE cannot be in the future')
            except ValueError as e:
                if "does not match format" in str(e):
                    raise ValueError('PURCHASE_DATE must be in YYYY-MM-DD format')
                raise
        return v

class AssetResponse(BaseModel):
    """Asset response model"""
    success: bool
    message: str
    asset_id: Optional[str] = None
    asset_data: Optional[dict] = None

class GoalCreate(BaseModel):
    employee_id: str
    goal_description: str
    target_date: str
    status: str = "Planned"


class TrainingCreate(BaseModel):
    employee_id: str
    training_name: str
    completion_date: str
    status: str = "Completed"
    skills_acquired: Optional[str] = None


class AppraisalSubmit(BaseModel):
    employee_id: str
    manager_id: str
    final_rating: float = Field(..., ge=0, le=5)
    comments: str
    appraisal_date: str


class CompetencySubmit(BaseModel):
    employee_id: str
    competency_name: str
    rating: float = Field(..., ge=0, le=5)
    comments: Optional[str] = None


class SkillSubmit(BaseModel):
    employee_id: str
    skill_name: str
    proficiency_level: str
    years_of_experience: Optional[float] = None
    last_used: Optional[str] = None


# ══════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════

app = FastAPI(title="Employee Microservice", version="1.0")


@app.on_event("startup")
async def _emp_startup():
    """Pre-warm Oracle pool; block until first connection ready so first request is fast."""
    _emp_pool_prefill()
    print("⏳ [emp-startup] waiting for first warm Oracle connection...")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: _emp_first_ready.wait(timeout=20))
    if _emp_first_ready.is_set():
        print("✅ [emp-startup] connection pool ready — accepting requests")
    else:
        print("⚠️ [emp-startup] pool not ready after 20s — starting anyway")


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.chakorahub.com", "https://chakorahub.com",
        "http://www.chakorahub.com",  "http://chakorahub.com",
        "http://127.0.0.1:8080",      "http://localhost:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════
# INTERNAL HIERARCHY HELPERS  (not route handlers)
# Renamed with _db_ prefix to avoid collision with async route fns.
# ══════════════════════════════════════════════════════════════

def _db_is_top_manager(conn, employee_id: str) -> bool:
    """Return True if employee has no manager (top of org)."""
    cursor = conn.cursor(DictCursor)
    cursor.execute(
        "SELECT MANAGER_ID FROM EMP_NRM_JOB_WORK WHERE EMPLOYEE_ID = %s",
        (employee_id,),
    )
    result = cursor.fetchone()
    cursor.close()
    return result is None or result.get("MANAGER_ID") is None


def _db_get_direct_manager_id(conn, employee_id: str) -> Optional[str]:
    """Return the direct manager's employee_id, or None."""
    cursor = conn.cursor(DictCursor)
    cursor.execute(
        "SELECT MANAGER_ID FROM EMP_NRM_JOB_WORK WHERE EMPLOYEE_ID = %s LIMIT 1",
        (employee_id,),
    )
    result = cursor.fetchone()
    cursor.close()
    return result["MANAGER_ID"] if result else None


def _db_get_manager_via_hierarchy(conn, employee_id: str) -> tuple[Optional[str], Optional[str]]:
    """Walk the EMP_NRM_JOB_WORK hierarchy recursively to find the nearest manager
    who has a non-null email.  Returns (manager_id, manager_email) or (None, None).
    Used as a fallback when the direct JOB_WORK row has no MANAGER_ID.
    """
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            WITH RECURSIVE mgr_chain AS (
                SELECT jw.EMPLOYEE_ID, jw.MANAGER_ID, 1 AS depth
                FROM EMP_NRM_JOB_WORK jw
                WHERE jw.EMPLOYEE_ID = %s AND jw.MANAGER_ID IS NOT NULL

                UNION ALL

                SELECT jw2.EMPLOYEE_ID, jw2.MANAGER_ID, mc.depth + 1
                FROM EMP_NRM_JOB_WORK jw2
                INNER JOIN mgr_chain mc ON jw2.EMPLOYEE_ID = mc.MANAGER_ID
                WHERE jw2.MANAGER_ID IS NOT NULL AND mc.depth < 10
            )
            SELECT mc.MANAGER_ID, e.EMAIL AS MANAGER_EMAIL
            FROM mgr_chain mc
            JOIN EMP_NRM_EMPLOYEES e ON mc.MANAGER_ID = e.EMPLOYEE_ID
            WHERE e.EMAIL IS NOT NULL AND e.EMAIL != ''
            ORDER BY mc.depth ASC
            LIMIT 1
            """,
            (employee_id,),
        )
        row = cursor.fetchone()
        if row:
            return row["MANAGER_ID"], row["MANAGER_EMAIL"]
        return None, None
    finally:
        cursor.close()


def _db_get_direct_reportees(conn, manager_employee_id: str) -> List[Dict]:
    """Return direct reportees of a manager."""
    cursor = conn.cursor(DictCursor)
    cursor.execute(
        "SELECT MANAGER_ID FROM EMP_NRM_JOB_WORK WHERE EMPLOYEE_ID = %s",
        (manager_employee_id,),
    )
    mgr = cursor.fetchone()
    if not mgr or not mgr.get("MANAGER_ID"):
        cursor.close()
        return []
    manager_id = mgr["MANAGER_ID"]
    cursor.execute(
        """
        SELECT
            e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.EMAIL,
            d.DEPT_NAME, des.TITLE AS DESIGNATION
        FROM EMP_NRM_JOB_WORK jw
        JOIN EMP_NRM_EMPLOYEES e  ON jw.EMPLOYEE_ID   = e.EMPLOYEE_ID
        LEFT JOIN EMP_NRM_DEPARTMENTS   d   ON jw.DEPT_ID        = d.DEPT_ID
        LEFT JOIN EMP_NRM_DESIGNATIONS  des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
        WHERE jw.MANAGER_ID = %s AND e.STATUS = 'ACTIVE'
        ORDER BY e.EMPLOYEE_NAME
        """,
        (manager_id,),
    )
    data = cursor.fetchall()
    cursor.close()
    return data


def _db_is_manager(conn, employee_id: str) -> bool:
    """Return True if employee has any active direct reportees."""
    cached_roles = get_cached_authorization_roles(employee_id)
    if isinstance(cached_roles, dict) and "is_manager" in cached_roles:
        return bool(cached_roles["is_manager"])

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM EMP_NRM_JOB_WORK jw
            JOIN EMP_NRM_EMPLOYEES e ON jw.EMPLOYEE_ID = e.EMPLOYEE_ID
            WHERE jw.MANAGER_ID = %(eid)s AND e.STATUS = 'ACTIVE'
            """,
            {"eid": employee_id},
        )
        result = cursor.fetchone()
        is_mgr = bool(result and result["CNT"] > 0)
        cache_authorization_roles(employee_id, is_mgr)
        return is_mgr
    finally:
        cursor.close()


def _db_can_appraise(conn, appraiser_id: str, employee_id: str) -> bool:
    """Return True if appraiser_id is a direct manager or top-level manager."""
    cursor = conn.cursor(DictCursor)
    # Direct manager check
    cursor.execute(
        "SELECT 1 FROM EMP_NRM_JOB_WORK WHERE EMPLOYEE_ID = %s AND MANAGER_ID = %s",
        (employee_id, appraiser_id),
    )
    if cursor.fetchone():
        cursor.close()
        return True
    # Top-level manager check (has reportees)
    cursor.execute(
        "SELECT COUNT(*) AS CNT FROM EMP_NRM_JOB_WORK WHERE MANAGER_ID = %s",
        (appraiser_id,),
    )
    result = cursor.fetchone()
    cursor.close()
    return bool(result and result["CNT"] > 0)


def _db_get_all_subordinates(conn, manager_id: str) -> List[Dict]:
    """Return all subordinates recursively using a recursive CTE."""
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            WITH RECURSIVE subordinate_tree AS (
                SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, jw.MANAGER_ID, 1 AS LEVEL
                FROM EMP_NRM_JOB_WORK jw
                JOIN EMP_NRM_EMPLOYEES e ON jw.EMPLOYEE_ID = e.EMPLOYEE_ID
                WHERE jw.MANAGER_ID = %s AND e.STATUS = 'ACTIVE'

                UNION ALL

                SELECT e2.EMPLOYEE_ID, e2.EMPLOYEE_NAME, jw2.MANAGER_ID, st.LEVEL + 1
                FROM subordinate_tree st
                JOIN EMP_NRM_JOB_WORK jw2 ON st.EMPLOYEE_ID = jw2.MANAGER_ID
                JOIN EMP_NRM_EMPLOYEES e2 ON jw2.EMPLOYEE_ID = e2.EMPLOYEE_ID
                WHERE e2.STATUS = 'ACTIVE'
            )
            SELECT EMPLOYEE_ID, EMPLOYEE_NAME, LEVEL
            FROM subordinate_tree
            ORDER BY LEVEL, EMPLOYEE_NAME
            """,
            (manager_id,),
        )
        return cursor.fetchall()
    finally:
        cursor.close()


# ══════════════════════════════════════════════════════════════
# ROUTES — HEALTH
# ══════════════════════════════════════════════════════════════

@app.get("/health")
def health_check():
    redis_health = _redis_request("GET", "/health")
    return {
        "status": "healthy",
        "service": "employee",
        "redis_cache": "connected" if redis_health.get("success") else "unavailable",
    }


# ══════════════════════════════════════════════════════════════
# ROUTES — LOGIN
# ══════════════════════════════════════════════════════════════

@app.post("/api/employee/login", response_model=EmployeeLoginResponse)
def employee_login(data: EmployeeLogin, request: Request):
    clean_id = (data.employee_id or "").strip().upper()
    client_id = get_client_identifier(request, clean_id or "UNKNOWN")
    rate_limit_key = cache_key("rate_login", client_id)
    if not redis_rate_limit_allow(rate_limit_key, limit=10, window_seconds=60):
        raise HTTPException(429, "Too many login attempts. Please retry after a minute")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)

        # Step 1 — credentials
        cursor.execute(
            "SELECT EMPLOYEE_ID, PASSWORD FROM EMP_NRM_LOGINS WHERE EMPLOYEE_ID = %s LIMIT 1",
            (clean_id,),
        )
        login_row = cursor.fetchone()
        if not login_row:
            raise HTTPException(400, "Employee ID not found")

        db_password = (login_row.get("PASSWORD") or "").strip()
        if db_password != data.password:
            raise HTTPException(401, "Invalid password")

        # Step 2 — employee master
        cursor.execute(
            "SELECT APPLICATION_ID, EMPLOYEE_NAME, EMAIL, STATUS FROM EMP_NRM_EMPLOYEES WHERE EMPLOYEE_ID = %s LIMIT 1",
            (clean_id,),
        )
        emp = cursor.fetchone()
        if not emp:
            raise HTTPException(404, "Employee data not found")
        if emp.get("STATUS") and emp["STATUS"].upper() != "ACTIVE":
            raise HTTPException(403, "Employee account is not active")

        # Step 3 — personal info
        cursor.execute(
            "SELECT FIRST_NAME, LAST_NAME, PROFILE_PIC FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s LIMIT 1",
            (clean_id,),
        )
        personal = cursor.fetchone() or {}

        display_name = emp.get("EMPLOYEE_NAME")
        if not display_name:
            first = personal.get("FIRST_NAME", "").strip()
            last = personal.get("LAST_NAME", "").strip()
            display_name = f"{first} {last}".strip() or "Employee"

        profile_pic = personal.get("PROFILE_PIC") or "profile_photo/defaultpicture.jpg"

        # Step 4 — job info
        cursor.execute(
            """
            SELECT d.DEPT_NAME, des.TITLE
            FROM EMP_NRM_JOB_WORK j
            LEFT JOIN EMP_NRM_DEPARTMENTS   d   ON j.DEPT_ID        = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS  des ON j.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE j.EMPLOYEE_ID = %s LIMIT 1
            """,
            (clean_id,),
        )
        job = cursor.fetchone() or {}
        manager_flag = _db_is_manager(conn, clean_id)

        login_payload = {
            "success": True,
            "message": "Login successful",
            "employee_id": clean_id,
            "employee_db_id": emp.get("APPLICATION_ID"),
            "employee_name": display_name,
            "employee_email": emp.get("EMAIL", ""),
            "employee_department": job.get("DEPT_NAME", "N/A"),
            "employee_designation": job.get("TITLE", "N/A"),
            "profile_pic": profile_pic,
            "is_manager": manager_flag,
        }

        # Populate all three caches atomically
        cache_session(clean_id, login_payload)
        cache_user_profile(
            clean_id,
            {
                "employee_name": display_name,
                "employee_email": emp.get("EMAIL", ""),
                "employee_department": job.get("DEPT_NAME", "N/A"),
                "employee_designation": job.get("TITLE", "N/A"),
                "profile_pic": profile_pic,
            },
        )
        cache_authorization_roles(clean_id, manager_flag)

        cursor.close()
        print(f"✅ Employee login: {clean_id} — {display_name}")

        return EmployeeLoginResponse(
            success=True,
            message="Login successful",
            employee_id=clean_id,
            employee_db_id=emp.get("APPLICATION_ID"),
            employee_name=display_name,
            employee_email=emp.get("EMAIL", ""),
            employee_department=job.get("DEPT_NAME", "N/A"),
            employee_designation=job.get("TITLE", "N/A"),
            profile_pic=profile_pic,
            is_manager=manager_flag,
        )

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Login error: {str(e)}")
    finally:
        try:
            _emp_pool_return(conn)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# ROUTES — APPRAISAL
# ══════════════════════════════════════════════════════════════

@app.get("/api/appraisal/employee/{employee_id}")
def get_employee_appraisal_data(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_employee", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)

        cursor.execute(
            """
            SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.EMAIL, d.DEPT_NAME, des.TITLE AS DESIGNATION
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw  ON e.EMPLOYEE_ID   = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d   ON jw.DEPT_ID   = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE e.EMPLOYEE_ID = %s LIMIT 1
            """,
            (clean_id,),
        )
        employee_info = cursor.fetchone()
        if not employee_info:
            raise HTTPException(404, f"Employee not found: {employee_id}")

        cursor.execute(
            "SELECT FINAL_RATING, COMMENTS, APPRAISAL_DATE, SUBMITTED_BY FROM EMP_NRM_APPRAISAL_SUMMARY WHERE EMPLOYEE_ID = %s ORDER BY APPRAISAL_DATE DESC LIMIT 1",
            (clean_id,),
        )
        summary = cursor.fetchone()

        cursor.execute(
            "SELECT GOAL_ID, GOAL_DESCRIPTION, TARGET_DATE, STATUS, CREATED_DATE FROM EMP_NRM_APPRAISAL_GOALS WHERE EMPLOYEE_ID = %s ORDER BY CREATED_DATE DESC LIMIT 10",
            (clean_id,),
        )
        goals = cursor.fetchall() or []

        cursor.execute(
            "SELECT TRAINING_ID, TRAINING_NAME, COMPLETION_DATE, STATUS, SKILLS_ACQUIRED FROM EMP_NRM_APPRAISAL_TRAININGS WHERE EMPLOYEE_ID = %s ORDER BY CREATED_DATE DESC LIMIT 10",
            (clean_id,),
        )
        trainings = cursor.fetchall() or []

        cursor.execute(
            "SELECT COMPETENCY_NAME, RATING, COMMENTS FROM EMP_NRM_APPRAISAL_COMPETENCIES WHERE EMPLOYEE_ID = %s ORDER BY CREATED_DATE DESC LIMIT 10",
            (clean_id,),
        )
        competencies = cursor.fetchall() or []

        cursor.execute(
            "SELECT SKILL_NAME, PROFICIENCY_LEVEL, YEARS_OF_EXPERIENCE, LAST_USED FROM EMP_NRM_APPRAISAL_SKILLS WHERE EMPLOYEE_ID = %s ORDER BY CREATED_DATE DESC LIMIT 10",
            (clean_id,),
        )
        skills = cursor.fetchall() or []

        payload = {
            "success": True,
            "employee_info": employee_info,
            "summary": summary,
            "goals": goals,
            "trainings": trainings,
            "competencies": competencies,
            "skills": skills,
        }
        redis_json_setex_safe(ck, CACHE_TTL_MEDIUM, payload)
        return payload

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, "Internal Server Error")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.get("/api/appraisal/summary/{employee_id}")
def get_appraisal_summary(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_summary", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT e.EMPLOYEE_NAME, d.DEPT_NAME, des.TITLE AS DESIGNATION,
                   a.FINAL_RATING, a.COMMENTS, a.APPRAISAL_DATE
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw  ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d   ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            LEFT JOIN EMP_NRM_APPRAISAL_SUMMARY a ON e.EMPLOYEE_ID = a.EMPLOYEE_ID
            WHERE e.EMPLOYEE_ID = %s
            ORDER BY a.APPRAISAL_DATE DESC LIMIT 1
            """,
            (clean_id,),
        )
        appraisal_data = cursor.fetchone() or {}

        cursor.execute("SELECT COUNT(*) CNT FROM EMP_NRM_APPRAISAL_GOALS WHERE EMPLOYEE_ID = %s", (clean_id,))
        goals_count = (cursor.fetchone() or {}).get("CNT", 0)

        cursor.execute("SELECT COUNT(*) CNT FROM EMP_NRM_APPRAISAL_TRAININGS WHERE EMPLOYEE_ID = %s AND STATUS = 'Completed'", (clean_id,))
        trainings_count = (cursor.fetchone() or {}).get("CNT", 0)

        summary = {
            "employee_info": {
                "name": appraisal_data.get("EMPLOYEE_NAME", ""),
                "department": appraisal_data.get("DEPT_NAME", ""),
                "designation": appraisal_data.get("DESIGNATION", ""),
            },
            "appraisal_data": appraisal_data,
            "stats": {"goals_count": goals_count, "trainings_count": trainings_count},
        }
        payload = {"success": True, "summary": summary}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.get("/api/appraisal/goals/{employee_id}")
def get_appraisal_goals(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_goals", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            "SELECT GOAL_ID, GOAL_DESCRIPTION, TARGET_DATE, STATUS, CREATED_DATE, UPDATED_DATE FROM EMP_NRM_APPRAISAL_GOALS WHERE EMPLOYEE_ID = %s ORDER BY CREATED_DATE DESC",
            (clean_id,),
        )
        goals = cursor.fetchall() or []
        payload = {"success": True, "goals": goals}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/goals/{employee_id}")
def create_appraisal_goal(employee_id: str, data: Dict[str, Any]):
    clean_id = employee_id.strip().upper()
    goal_description = (data.get("goal_description") or "").strip()
    target_date = data.get("target_date")
    status = (data.get("status") or "Planned").strip()

    if not goal_description or not target_date:
        raise HTTPException(400, "Goal description and target date are required")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_GOALS (EMPLOYEE_ID, GOAL_DESCRIPTION, TARGET_DATE, STATUS) VALUES (%s, %s, %s, %s)",
            (clean_id, goal_description, target_date, status),
        )
        conn.commit()
        redis_delete_safe(cache_key("appraisal_goals", clean_id))
        redis_delete_safe(cache_key("appraisal_employee", clean_id))
        return {"success": True, "message": "Goal added successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.get("/api/appraisal/trainings/{employee_id}")
def get_appraisal_trainings(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_trainings", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            "SELECT TRAINING_ID, TRAINING_NAME, COMPLETION_DATE, STATUS, SKILLS_ACQUIRED, CREATED_DATE, UPDATED_DATE FROM EMP_NRM_APPRAISAL_TRAININGS WHERE EMPLOYEE_ID = %s ORDER BY CREATED_DATE DESC",
            (clean_id,),
        )
        trainings = cursor.fetchall() or []
        payload = {"success": True, "trainings": trainings}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/trainings/{employee_id}")
def create_appraisal_training(employee_id: str, data: Dict[str, Any]):
    clean_id = employee_id.strip().upper()
    training_name = (data.get("training_name") or "").strip()
    completion_date = data.get("completion_date")
    status = (data.get("status") or "Completed").strip()
    skills_acquired = (data.get("skills_acquired") or "").strip()

    if not training_name or not completion_date:
        raise HTTPException(400, "Training name and completion date required")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_TRAININGS (EMPLOYEE_ID, TRAINING_NAME, COMPLETION_DATE, STATUS, SKILLS_ACQUIRED) VALUES (%s, %s, %s, %s, %s)",
            (clean_id, training_name, completion_date, status, skills_acquired),
        )
        conn.commit()
        redis_delete_safe(cache_key("appraisal_trainings", clean_id))
        redis_delete_safe(cache_key("appraisal_employee", clean_id))
        return {"success": True, "message": "Training added successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/submit")
def submit_appraisal(data: AppraisalSubmit):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        if not _db_can_appraise(conn, data.manager_id, data.employee_id):
            raise HTTPException(403, "You can only appraise your direct reportees")

        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_SUMMARY (EMPLOYEE_ID, FINAL_RATING, COMMENTS, APPRAISAL_DATE, SUBMITTED_BY) VALUES (%s, %s, %s, %s, %s)",
            (data.employee_id, data.final_rating, data.comments, data.appraisal_date, data.manager_id or None),
        )
        conn.commit()
        cursor.close()
        # Invalidate relevant caches
        redis_delete_safe(cache_key("appraisal_employee", data.employee_id))
        redis_delete_safe(cache_key("appraisal_summary", data.employee_id))
        redis_delete_safe(cache_key("appraisal_viewable", data.manager_id))
        return {"success": True, "message": "Appraisal submitted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/goals/add")
def add_goal(data: GoalCreate):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_GOALS (EMPLOYEE_ID, GOAL_DESCRIPTION, TARGET_DATE, STATUS) VALUES (%s, %s, %s, %s)",
            (data.employee_id, data.goal_description, data.target_date, data.status),
        )
        conn.commit()
        cursor.close()
        redis_delete_safe(cache_key("appraisal_goals", data.employee_id))
        redis_delete_safe(cache_key("appraisal_employee", data.employee_id))
        return {"success": True, "message": "Goal added successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/trainings/add")
def add_training(data: TrainingCreate):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_TRAININGS (EMPLOYEE_ID, TRAINING_NAME, COMPLETION_DATE, STATUS, SKILLS_ACQUIRED) VALUES (%s, %s, %s, %s, %s)",
            (data.employee_id, data.training_name, data.completion_date, data.status, data.skills_acquired),
        )
        conn.commit()
        cursor.close()
        redis_delete_safe(cache_key("appraisal_trainings", data.employee_id))
        redis_delete_safe(cache_key("appraisal_employee", data.employee_id))
        return {"success": True, "message": "Training added successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/competencies/add")
def add_competency(data: CompetencySubmit):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_COMPETENCIES (EMPLOYEE_ID, COMPETENCY_NAME, RATING, COMMENTS) VALUES (%s, %s, %s, %s)",
            (data.employee_id, data.competency_name, data.rating, data.comments),
        )
        conn.commit()
        cursor.close()
        redis_delete_safe(cache_key("appraisal_employee", data.employee_id))
        return {"success": True, "message": "Competency added successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/appraisal/skills/add")
def add_skill(data: SkillSubmit):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_APPRAISAL_SKILLS (EMPLOYEE_ID, SKILL_NAME, PROFICIENCY_LEVEL, YEARS_OF_EXPERIENCE, LAST_USED) VALUES (%s, %s, %s, %s, %s)",
            (data.employee_id, data.skill_name, data.proficiency_level, data.years_of_experience, data.last_used),
        )
        conn.commit()
        cursor.close()
        redis_delete_safe(cache_key("appraisal_employee", data.employee_id))
        return {"success": True, "message": "Skill added successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# ROUTES — ID CARD
# ══════════════════════════════════════════════════════════════

@app.get("/api/employee/id-card/{employee_id}")
def get_employee_id_card(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("employee_id_card", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)

        cursor.execute("SELECT ID_NUMBER, ISSUE_DATE, EXPIRY_DATE FROM EMP_NRM_IDCARD WHERE EMPLOYEE_ID = %s", (clean_id,))
        id_card_data = cursor.fetchone()

        cursor.execute("SELECT FIRST_NAME, LAST_NAME, DOB, EMAIL, PHONE, ADDRESS, PROFILE_PIC FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s", (clean_id,))
        personal_data = cursor.fetchone()

        cursor.execute(
            """
            SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.STATUS, d.DEPT_NAME, des.TITLE AS DESIGNATION
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw  ON e.EMPLOYEE_ID   = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d   ON jw.DEPT_ID   = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE e.EMPLOYEE_ID = %s
            """,
            (clean_id,),
        )
        employee_data = cursor.fetchone()

        payload = {
            "success": True,
            "id_card_data": id_card_data,
            "personal_data": personal_data,
            "employee_data": employee_data,
        }
        redis_json_setex_safe(ck, CACHE_TTL_MEDIUM, payload)
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# ROUTES — EMPLOYEE QUERIES (HR helpdesk)
# ══════════════════════════════════════════════════════════════

@app.get("/api/employee/queries/{employee_id}")
def get_employee_queries(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("employee_queries", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            "SELECT QUERY_ID, QUERY_TEXT, STATUS, CREATED_AT FROM EMP_NRM_QUERIES WHERE EMPLOYEE_ID = %s ORDER BY CREATED_AT DESC",
            (clean_id,),
        )
        queries_data = cursor.fetchall() or []
        payload = {"success": True, "queries_data": queries_data}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/employee/queries/{employee_id}")
def create_employee_query(employee_id: str, query_text: str = Form(...)):
    clean_id = employee_id.strip().upper()
    text = (query_text or "").strip()
    if not text:
        raise HTTPException(400, "query_text is required")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = None
    try:
        import uuid
        query_id = f"QRY-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO EMP_NRM_QUERIES (QUERY_ID, EMPLOYEE_ID, QUERY_TEXT, STATUS, CREATED_AT) VALUES (%s, %s, %s, 'Pending', CURRENT_TIMESTAMP())",
            (query_id, clean_id, text),
        )
        conn.commit()
        redis_delete_safe(cache_key("employee_queries", clean_id))

        # Notify admin via email
        try:
            cursor2 = conn.cursor(DictCursor)
            cursor2.execute(
                "SELECT EMPLOYEE_NAME, EMAIL FROM EMP_NRM_EMPLOYEES WHERE EMPLOYEE_ID = %s LIMIT 1",
                (clean_id,),
            )
            emp = cursor2.fetchone()
            cursor2.close()
            emp_name  = emp.get("EMPLOYEE_NAME", clean_id) if emp else clean_id
            emp_email = emp.get("EMAIL", "") if emp else ""
        except Exception:
            emp_name, emp_email = clean_id, ""
        _send_query_email(clean_id, emp_name, emp_email, query_id, text)

        return {"success": True, "message": "Query submitted successfully"}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# ROUTES — LEAVE
# ══════════════════════════════════════════════════════════════

# ── Shared SES builder ────────────────────────────────────────────────────────
def _ses_client():
    return boto3.client(
        "ses",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_SES_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SES_SECRET_ACCESS_KEY"),
    )

def _admin_email() -> str:
    return os.getenv("ADMIN_EMAIL", "admin@chakorahub.com")

# ── Shared HTML email shell (matches admin-register template) ─────────────────
def _email_html(icon: str, heading: str, subheading: str, rows_html: str,
                footer_note: str = "", action_html: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{heading} – ChakoraHub</title></head>
<body style="margin:0;padding:0;background-color:#eef2f7;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#eef2f7;padding:32px 0;">
    <tr><td align="center">
      <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"
             style="max-width:600px;width:100%;background-color:#ffffff;border-radius:12px;
                    overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
        <!-- HEADER -->
        <tr><td align="center" style="padding:36px 32px 24px;">
          <div style="font-size:48px;line-height:1;margin-bottom:8px;">{icon}</div>
          <div style="font-size:18px;font-weight:700;color:#1a2340;letter-spacing:0.04em;margin-bottom:10px;">ChakoraHub</div>
          <div style="font-size:15px;color:#5b9bd5;font-weight:500;">{subheading}</div>
        </td></tr>
        <!-- BODY -->
        <tr><td style="padding:0 32px 32px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                 style="background-color:#f0f5fb;border-radius:10px;border-left:4px solid #4a90d9;overflow:hidden;">
            <tr><td style="padding:20px 24px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                {rows_html}
              </table>
            </td></tr>
          </table>
          {action_html}
        </td></tr>
        <!-- FOOTER -->
        <tr><td align="center" style="padding:20px 32px 36px;border-top:1px solid #edf2f7;">
          <p style="margin:0;font-size:13px;color:#a0aec0;line-height:1.7;">
            {footer_note}Questions? Write to
            <a href="mailto:{_admin_email()}" style="color:#5b9bd5;text-decoration:none;font-weight:600;">{_admin_email()}</a>
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

def _row(icon: str, label: str, value: str) -> str:
    return f"""<tr>
      <td width="36" valign="middle" style="padding:10px 8px 10px 0;font-size:18px;">{icon}</td>
      <td width="160" valign="middle" style="padding:10px 8px;font-size:14px;font-weight:700;color:#2d3748;">{label}</td>
      <td valign="middle" style="padding:10px 8px;font-size:14px;color:#2d3748;">{value}</td>
    </tr>"""

# ── Leave approval token helpers ──────────────────────────────────────────────
_LEAVE_SIGNING_SECRET = os.getenv("LEAVE_SIGNING_SECRET", "chakorahub-leave-secret-change-me")

def _sign_leave_token(leave_id: int, action: str) -> str:
    """HMAC-SHA256 token: leave_id|action|sig (URL-safe base64)."""
    import base64
    payload = f"{leave_id}|{action}"
    sig = hmac.new(_LEAVE_SIGNING_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    token = base64.urlsafe_b64encode(f"{payload}|{sig}".encode()).decode()
    return token

def _verify_leave_token(token: str):
    """Returns (leave_id, action) or raises ValueError."""
    import base64
    try:
        decoded = base64.urlsafe_b64decode(token.encode()).decode()
        leave_id_str, action, sig = decoded.rsplit("|", 2)
        payload = f"{leave_id_str}|{action}"
        expected = hmac.new(_LEAVE_SIGNING_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            raise ValueError("Invalid token signature")
        if action not in ("approve", "reject"):
            raise ValueError("Invalid action")
        return int(leave_id_str), action
    except Exception as e:
        raise ValueError(f"Invalid token: {e}")

# ── Leave email to manager (rich HTML + one-click approve/reject links) ────────
def _send_leave_email(to_email: str, employee_name: str, start_date, end_date,
                      reason: str, leave_id: int = 0, leave_type: str = "",
                      employee_email: str = "") -> None:
    """Send leave application confirmation to employee + approval request to manager."""
    def _send():
        try:
            sender    = _admin_email()
            base_url  = os.getenv("PUBLIC_APP_URL", "https://www.chakorahub.com")
            ses       = _ses_client()
            approve_token = _sign_leave_token(leave_id, "approve") if leave_id else ""
            reject_token  = _sign_leave_token(leave_id, "reject")  if leave_id else ""
            action_base   = f"{base_url}/manager/leave-action"

            leave_rows = (
                _row("📋", "Leave Type", leave_type or "—") +
                _row("📅", "From",       str(start_date)) +
                _row("📅", "To",         str(end_date)) +
                _row("📝", "Reason",     reason or "—")
            )

            # ── 1. Confirmation email → Employee ──────────────────────────────
            if employee_email:
                emp_rows = (
                    _row("🔖", "Leave ID",   str(leave_id) if leave_id else "—") +
                    leave_rows +
                    _row("📊", "Status",     "Pending — awaiting manager approval")
                )
                emp_html = _email_html(
                    icon="📅",
                    heading="Leave Application Received",
                    subheading="Your leave request has been submitted successfully",
                    rows_html=emp_rows,
                    footer_note="You will be notified once your manager reviews the request. ",
                )
                emp_text = (
                    f"Hi {employee_name},\n\n"
                    f"Your leave application has been submitted.\n\n"
                    f"Leave ID  : {leave_id or '—'}\n"
                    f"Type      : {leave_type or '—'}\n"
                    f"From      : {start_date}  To: {end_date}\n"
                    f"Reason    : {reason}\n"
                    f"Status    : Pending\n\n"
                    f"You will be notified once your manager reviews it.\n\nRegards,\nChakoraHub Team"
                )
                ses.send_email(
                    Source=sender,
                    Destination={"ToAddresses": [employee_email]},
                    Message={
                        "Subject": {"Data": f"Leave Application Submitted – {leave_type or 'Leave'} ({start_date} to {end_date})"},
                        "Body": {"Html": {"Data": emp_html}, "Text": {"Data": emp_text}},
                    },
                )
                print(f"✅ Leave confirmation sent to employee: {employee_email}")

            # ── 2. Approval request → Manager ─────────────────────────────────
            if to_email:
                mgr_rows = (
                    _row("👤", "Employee", employee_name) +
                    leave_rows
                )
                action_html = ""
                if leave_id:
                    action_html = f"""
                    <div style="margin-top:24px;text-align:center;">
                      <a href="{action_base}?token={approve_token}"
                         style="display:inline-block;padding:12px 28px;background:#22c55e;color:#fff;
                                font-weight:700;font-size:14px;border-radius:8px;text-decoration:none;margin-right:12px;">
                        ✅ Approve
                      </a>
                      <a href="{action_base}?token={reject_token}"
                         style="display:inline-block;padding:12px 28px;background:#ef4444;color:#fff;
                                font-weight:700;font-size:14px;border-radius:8px;text-decoration:none;">
                        ❌ Reject
                      </a>
                    </div>"""
                mgr_html = _email_html(
                    icon="📋",
                    heading="Leave Request",
                    subheading=f"New Leave Request from {employee_name}",
                    rows_html=mgr_rows,
                    action_html=action_html,
                )
                mgr_text = (
                    f"Leave Request from {employee_name}\n"
                    f"Type: {leave_type}  From: {start_date}  To: {end_date}\n"
                    f"Reason: {reason}\n\n"
                    f"Approve: {action_base}?token={approve_token}\n"
                    f"Reject:  {action_base}?token={reject_token}"
                )
                ses.send_email(
                    Source=sender,
                    Destination={"ToAddresses": [to_email]},
                    Message={
                        "Subject": {"Data": f"Leave Request from {employee_name}"},
                        "Body": {"Html": {"Data": mgr_html}, "Text": {"Data": mgr_text}},
                    },
                )
                print(f"✅ Leave approval request sent to manager: {to_email}")

        except Exception as e:
            print("❌ Leave email error (non-fatal):", e)
    threading.Thread(target=_send, daemon=True).start()


# ── Query notification email to admin ─────────────────────────────────────────
def _send_query_email(employee_id: str, employee_name: str, employee_email: str,
                      query_id: str, query_text: str) -> None:
    """Send query confirmation to employee + notification to admin."""
    def _send():
        try:
            sender = _admin_email()
            ses = _ses_client()

            # ── 1. Confirmation email → Employee ──────────────────────────────
            if employee_email:
                emp_rows = (
                    _row("🔖", "Query ID",  query_id) +
                    _row("📝", "Your Query", query_text) +
                    _row("📊", "Status",     "Pending — our team will respond shortly")
                )
                emp_html = _email_html(
                    icon="✅",
                    heading="Query Received",
                    subheading="We've received your query and will get back to you soon",
                    rows_html=emp_rows,
                    footer_note="We aim to respond within 1–2 business days. ",
                )
                emp_text = (
                    f"Hi {employee_name},\n\n"
                    f"Your query has been received successfully.\n\n"
                    f"Query ID : {query_id}\n"
                    f"Query    : {query_text}\n"
                    f"Status   : Pending\n\n"
                    f"We aim to respond within 1–2 business days.\n\n"
                    f"Regards,\nChakoraHub Team"
                )
                ses.send_email(
                    Source=sender,
                    Destination={"ToAddresses": [employee_email]},
                    Message={
                        "Subject": {"Data": f"Query Received – {query_id}"},
                        "Body": {"Html": {"Data": emp_html}, "Text": {"Data": emp_text}},
                    },
                )
                print(f"✅ Query confirmation sent to employee: {employee_email}")

            # ── 2. Notification email → Admin ─────────────────────────────────
            admin_rows = (
                _row("👤", "Employee",    employee_name) +
                _row("🆔", "Employee ID", employee_id) +
                _row("📧", "Email",       employee_email or "—") +
                _row("🔖", "Query ID",    query_id) +
                _row("📝", "Query",       query_text)
            )
            admin_html = _email_html(
                icon="❓",
                heading="New Employee Query",
                subheading="A new employee query has been submitted",
                rows_html=admin_rows,
            )
            admin_text = (
                f"New Employee Query\nFrom: {employee_name} ({employee_id})\n"
                f"Email: {employee_email or '—'}\nQuery ID: {query_id}\n\n{query_text}"
            )
            ses.send_email(
                Source=sender,
                Destination={"ToAddresses": [sender]},
                Message={
                    "Subject": {"Data": f"Employee Query from {employee_name} [{query_id}]"},
                    "Body": {"Html": {"Data": admin_html}, "Text": {"Data": admin_text}},
                },
            )
            print("✅ Query notification sent to admin")

        except Exception as e:
            print("❌ Query email error (non-fatal):", e)
    threading.Thread(target=_send, daemon=True).start()


# ── Payslip download notification email ───────────────────────────────────────
def _send_payslip_email(employee_id: str, employee_name: str, employee_email: str,
                        month: str, year: str) -> None:
    """Send payslip download confirmation to employee + notification to admin."""
    def _send():
        try:
            from datetime import datetime as _dt
            sender        = _admin_email()
            ses           = _ses_client()
            downloaded_at = _dt.now().strftime("%d %b %Y, %H:%M")

            # ── 1. Confirmation email → Employee ──────────────────────────────
            if employee_email:
                emp_rows = (
                    _row("📅", "Pay Period",    f"{month} {year}") +
                    _row("⏰", "Downloaded At", downloaded_at) +
                    _row("📊", "Status",        "Successfully downloaded")
                )
                emp_html = _email_html(
                    icon="💰",
                    heading="Payslip Downloaded",
                    subheading=f"Your payslip for {month} {year} has been downloaded",
                    rows_html=emp_rows,
                    footer_note="If you did not request this download, please contact HR immediately. ",
                )
                emp_text = (
                    f"Hi {employee_name},\n\n"
                    f"Your payslip for {month} {year} was downloaded at {downloaded_at}.\n\n"
                    f"If you did not initiate this, please contact HR immediately.\n\n"
                    f"Regards,\nChakoraHub Team"
                )
                ses.send_email(
                    Source=sender,
                    Destination={"ToAddresses": [employee_email]},
                    Message={
                        "Subject": {"Data": f"Payslip Downloaded – {month} {year}"},
                        "Body": {"Html": {"Data": emp_html}, "Text": {"Data": emp_text}},
                    },
                )
                print(f"✅ Payslip confirmation sent to employee: {employee_email}")

            # ── 2. Notification email → Admin ─────────────────────────────────
            admin_rows = (
                _row("👤", "Employee",       employee_name) +
                _row("🆔", "Employee ID",    employee_id) +
                _row("📧", "Email",          employee_email or "—") +
                _row("📅", "Pay Period",     f"{month} {year}") +
                _row("⏰", "Downloaded At",  downloaded_at)
            )
            admin_html = _email_html(
                icon="💰",
                heading="Payslip Downloaded",
                subheading="An employee has downloaded their payslip",
                rows_html=admin_rows,
            )
            ses.send_email(
                Source=sender,
                Destination={"ToAddresses": [sender]},
                Message={
                    "Subject": {"Data": f"Payslip Downloaded – {employee_name} ({month} {year})"},
                    "Body": {"Html": {"Data": admin_html},
                             "Text": {"Data": f"Payslip downloaded by {employee_name} ({employee_id}) for {month} {year} at {downloaded_at}"}},
                },
            )
            print("✅ Payslip notification sent to admin")

        except Exception as e:
            print("❌ Payslip email error (non-fatal):", e)
    threading.Thread(target=_send, daemon=True).start()


@app.post("/api/employee/apply-leave")
async def apply_leave(
    request: Request,
    employee_id: str = Form(...),
    leave_type: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    reason: str = Form(...),
):
    client_id = get_client_identifier(request, employee_id)
    rate_limit_key = cache_key("rate_apply_leave", client_id)
    if not redis_rate_limit_allow(rate_limit_key, limit=20, window_seconds=60):
        raise HTTPException(429, "Too many leave requests. Please retry after a minute")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "DB connection failed")

        cursor = conn.cursor(DictCursor)

        leave_type = (leave_type or "").strip()
        if not leave_type:
            raise HTTPException(400, "Leave type is required")

        try:
            start_dt = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
            end_dt   = date.fromisoformat(end_date)   if isinstance(end_date, str)   else end_date
        except Exception as e:
            raise HTTPException(400, f"Invalid date format: {str(e)}")

        if start_dt > end_dt:
            raise HTTPException(400, "End date must be after start date")

        total_days = int((end_dt - start_dt).days + 1)

        cursor.execute(
            """
            SELECT e.EMPLOYEE_NAME, e.EMAIL AS EMP_EMAIL,
                   jw.MANAGER_ID, m.EMAIL AS MANAGER_EMAIL
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_EMPLOYEES m  ON jw.MANAGER_ID = m.EMPLOYEE_ID
            WHERE e.EMPLOYEE_ID = %s
            """,
            (employee_id,),
        )
        emp = cursor.fetchone()
        if not emp:
            raise HTTPException(404, "Employee not found")

        employee_name  = emp["EMPLOYEE_NAME"]
        employee_email = emp.get("EMP_EMAIL", "") or ""
        manager_id    = emp.get("MANAGER_ID")
        manager_email = emp.get("MANAGER_EMAIL")

        # Fallback: walk the hierarchy when the direct JOB_WORK row has no manager
        if not manager_id or not manager_email:
            fb_mgr_id, fb_mgr_email = _db_get_manager_via_hierarchy(conn, employee_id)
            if fb_mgr_id:
                manager_id    = manager_id    or fb_mgr_id
                manager_email = manager_email or fb_mgr_email

        # Detect whether MANAGER_ID column exists in EMP_NRM_LEAVE
        cursor.execute(
            """
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
              AND TABLE_NAME = 'EMP_NRM_LEAVE'
              AND COLUMN_NAME = 'MANAGER_ID'
            LIMIT 1
            """
        )
        has_manager_id_col = cursor.fetchone() is not None

        if has_manager_id_col:
            cursor.execute(
                """
                INSERT INTO EMP_NRM_LEAVE
                    (EMPLOYEE_ID, MANAGER_ID, LEAVE_TYPE, START_DATE, END_DATE,
                     TOTAL_DAYS, REASON, STATUS, APPLIED_AT)
                SELECT %s, jw.MANAGER_ID, %s, %s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP()
                FROM EMP_NRM_JOB_WORK jw
                WHERE jw.EMPLOYEE_ID = %s LIMIT 1
                """,
                (employee_id, leave_type, start_date, end_date, total_days, reason, employee_id),
            )
        else:
            cursor.execute(
                "INSERT INTO EMP_NRM_LEAVE (EMPLOYEE_ID, LEAVE_TYPE, START_DATE, END_DATE, TOTAL_DAYS, REASON, STATUS, APPLIED_AT) VALUES (%s, %s, %s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP())",
                (employee_id, leave_type, start_date, end_date, total_days, reason),
            )
        conn.commit()

        # Bust all leave caches
        invalidate_leave(employee_id, manager_id)

        # Fetch newly inserted leave_id for one-click email links
        new_leave_id = 0
        try:
            cursor.execute(
                "SELECT LEAVE_ID FROM EMP_NRM_LEAVE WHERE EMPLOYEE_ID = %s ORDER BY APPLIED_AT DESC LIMIT 1",
                (employee_id,),
            )
            row = cursor.fetchone()
            if row:
                new_leave_id = row[0] if isinstance(row, (list, tuple)) else row.get("LEAVE_ID", 0)
        except Exception:
            pass

        if manager_email or employee_email:
            _send_leave_email(manager_email or "", employee_name, start_date, end_date, reason,
                              leave_id=new_leave_id, leave_type=leave_type,
                              employee_email=employee_email)

        return {"success": True, "message": "Leave applied successfully"}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Apply leave error:", e)
        raise HTTPException(500, str(e))
    finally:
        try:
            if cursor: cursor.close()
            if conn:   _emp_pool_return(conn)
        except Exception:
            pass


@app.get("/api/employee/leave-history")
async def leave_history(employee_id: str):
    clean_id = employee_id.strip().upper()
    # Check both canonical key and emp_service namespaced key
    ck = cache_key("leave_history", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached
    # Also check canonical leave:{id} key populated by cache_leave()
    canonical = get_cached_leave(clean_id)
    if canonical is not None:
        return canonical

    print(f"🟨 Redis MISS: {ck}")
    conn = None
    cursor = None
    try:
        conn    = get_db_connection()
        cursor  = conn.cursor()
        cursor.execute(
            "SELECT LEAVE_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT FROM EMP_NRM_LEAVE WHERE EMPLOYEE_ID = %s ORDER BY APPLIED_AT DESC",
            (clean_id,),
        )
        rows = cursor.fetchall()
        result = [
            {
                "leave_id":   r[0],
                "start_date": str(r[1]) if r[1] else None,
                "end_date":   str(r[2]) if r[2] else None,
                "reason":     r[3],
                "status":     r[4],
                "applied_at": str(r[5]) if r[5] else None,
            }
            for r in rows
        ]
        redis_json_setex_safe(ck, CACHE_TTL_MEDIUM, result)
        # Also populate the canonical leave:{id} key
        cache_leave(clean_id, result)
        return result
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try:
            if cursor: cursor.close()
            if conn:   _emp_pool_return(conn)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# ROUTE ALIASES — LEAVE (app.py calls these URL patterns)
# ══════════════════════════════════════════════════════════════

@app.post("/api/employee/leave/apply")
async def apply_leave_alias(request: Request):
    """Alias so app.py POST /api/employee/leave/apply works.
    Forwards to the same logic as apply_leave but accepts JSON body."""
    conn = None
    cursor = None
    try:
        data = await request.json()
        employee_id = (data.get("employee_id") or "").strip().upper()
        leave_type  = (data.get("leave_type")  or "").strip()
        start_date  = (data.get("from_date")   or data.get("start_date") or "").strip()
        end_date    = (data.get("to_date")     or data.get("end_date")   or "").strip()
        reason      = (data.get("reason")      or "").strip()

        if not all([employee_id, leave_type, start_date, end_date]):
            raise HTTPException(400, "employee_id, leave_type, from_date/start_date, to_date/end_date are required")

        try:
            start_dt = date.fromisoformat(start_date)
            end_dt   = date.fromisoformat(end_date)
        except Exception as e:
            raise HTTPException(400, f"Invalid date format: {e}")

        if start_dt > end_dt:
            raise HTTPException(400, "End date must be after start date")

        total_days = int((end_dt - start_dt).days + 1)

        conn   = get_db_connection()
        if not conn:
            raise HTTPException(500, "DB connection failed")
        cursor = conn.cursor(DictCursor)

        # Get employee name and manager info for email notification
        cursor.execute(
            """
            SELECT e.EMPLOYEE_NAME, e.EMAIL AS EMP_EMAIL,
                   jw.MANAGER_ID, m.EMAIL AS MANAGER_EMAIL
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_EMPLOYEES m  ON jw.MANAGER_ID = m.EMPLOYEE_ID
            WHERE e.EMPLOYEE_ID = %s LIMIT 1
            """,
            (employee_id,),
        )
        emp = cursor.fetchone()
        if not emp:
            raise HTTPException(404, "Employee not found")

        employee_name  = emp.get("EMPLOYEE_NAME", employee_id)
        employee_email = emp.get("EMP_EMAIL", "") or ""
        manager_id    = emp.get("MANAGER_ID")
        manager_email = emp.get("MANAGER_EMAIL")

        # Fallback: walk the hierarchy when the direct JOB_WORK row has no manager
        if not manager_id or not manager_email:
            fb_mgr_id, fb_mgr_email = _db_get_manager_via_hierarchy(conn, employee_id)
            if fb_mgr_id:
                manager_id    = manager_id    or fb_mgr_id
                manager_email = manager_email or fb_mgr_email

        # Detect whether MANAGER_ID column exists in EMP_NRM_LEAVE
        cursor.execute(
            """
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
              AND TABLE_NAME = 'EMP_NRM_LEAVE'
              AND COLUMN_NAME = 'MANAGER_ID'
            LIMIT 1
            """
        )
        has_manager_id_col = cursor.fetchone() is not None

        if has_manager_id_col:
            cursor.execute(
                """
                INSERT INTO EMP_NRM_LEAVE
                    (EMPLOYEE_ID, MANAGER_ID, LEAVE_TYPE, START_DATE, END_DATE,
                     TOTAL_DAYS, REASON, STATUS, APPLIED_AT)
                SELECT %s, jw.MANAGER_ID, %s, %s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP()
                FROM EMP_NRM_JOB_WORK jw
                WHERE jw.EMPLOYEE_ID = %s LIMIT 1
                """,
                (employee_id, leave_type, start_date, end_date, total_days, reason, employee_id),
            )
        else:
            cursor.execute(
                "INSERT INTO EMP_NRM_LEAVE (EMPLOYEE_ID, LEAVE_TYPE, START_DATE, END_DATE, TOTAL_DAYS, REASON, STATUS, APPLIED_AT) VALUES (%s, %s, %s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP())",
                (employee_id, leave_type, start_date, end_date, total_days, reason),
            )
        conn.commit()

        # Bust all leave caches so history shows immediately
        invalidate_leave(employee_id, manager_id)

        # Fetch newly inserted leave_id for one-click email links
        new_leave_id = 0
        try:
            cursor.execute(
                "SELECT LEAVE_ID FROM EMP_NRM_LEAVE WHERE EMPLOYEE_ID = %s ORDER BY APPLIED_AT DESC LIMIT 1",
                (employee_id,),
            )
            row = cursor.fetchone()
            if row:
                new_leave_id = row[0] if isinstance(row, (list, tuple)) else row.get("LEAVE_ID", 0)
        except Exception:
            pass

        if manager_email or employee_email:
            _send_leave_email(manager_email or "", employee_name, start_date, end_date, reason,
                              leave_id=new_leave_id, leave_type=leave_type,
                              employee_email=employee_email)

        return {"success": True, "message": "Leave applied successfully"}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Apply leave (alias) error:", e)
        raise HTTPException(500, str(e))
    finally:
        try:
            if cursor: cursor.close()
            if conn:   _emp_pool_return(conn)
        except Exception:
            pass


@app.get("/api/employee/leave/history")
async def leave_history_alias(employee_id: str):
    """Alias so app.py GET /api/employee/leave/history works."""
    clean_id = employee_id.strip().upper()
    ck = cache_key("leave_history", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached
    canonical = get_cached_leave(clean_id)
    if canonical is not None:
        return canonical

    print(f"🟨 Redis MISS: {ck}")
    conn   = None
    cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT LEAVE_ID, LEAVE_TYPE, START_DATE, END_DATE,
                   TOTAL_DAYS, REASON, STATUS, APPLIED_AT
            FROM EMP_NRM_LEAVE
            WHERE EMPLOYEE_ID = %s
            ORDER BY APPLIED_AT DESC
            """,
            (clean_id,),
        )
        rows = cursor.fetchall()
        result = [
            {
                "leave_id":   r[0],
                "leave_type": r[1],
                "start_date": str(r[2]) if r[2] else None,
                "end_date":   str(r[3]) if r[3] else None,
                "total_days": r[4],
                "reason":     r[5],
                "status":     r[6],
                "applied_at": str(r[7]) if r[7] else None,
            }
            for r in rows
        ]
        redis_json_setex_safe(ck, CACHE_TTL_MEDIUM, result)
        cache_leave(clean_id, result)
        return result
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try:
            if cursor: cursor.close()
            if conn:   _emp_pool_return(conn)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# ROUTES — FESTIVALS
# ══════════════════════════════════════════════════════════════

@app.get("/api/employee/festivals")
async def get_festivals(year: int, month: int):
    ck = cache_key("festivals", year, month)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = None
    cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT FESTIVAL_NAME, FESTIVAL_DATE FROM NRM_FESTIVALS WHERE EXTRACT(YEAR FROM FESTIVAL_DATE) = %s AND EXTRACT(MONTH FROM FESTIVAL_DATE) = %s",
            (year, month),
        )
        rows = cursor.fetchall()
        payload = [{"name": r[0], "date": str(r[1]) if r[1] else None} for r in rows]
        redis_json_setex_safe(ck, CACHE_TTL_DAY, payload)  # festivals don't change mid-day
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try:
            if cursor: cursor.close()
            if conn:   _emp_pool_return(conn)
        except Exception:
            pass


@app.get("/api/employee/birthdays")
async def get_birthdays(year: int, month: int):
    """Return employees whose birthday falls in the given month."""
    ck = cache_key("birthdays", year, month)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn   = None
    cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        # DATE_OF_BIRTH lives in EMP_NRM_PERSONAL
        cursor.execute(
            """
            SELECT e.EMPLOYEE_NAME, p.DATE_OF_BIRTH
            FROM EMP_NRM_PERSONAL p
            JOIN EMP_NRM_EMPLOYEES e ON p.EMPLOYEE_ID = e.EMPLOYEE_ID
            WHERE EXTRACT(MONTH FROM p.DATE_OF_BIRTH) = %s
              AND e.STATUS = 'ACTIVE'
            ORDER BY EXTRACT(DAY FROM p.DATE_OF_BIRTH)
            """,
            (month,),
        )
        rows = cursor.fetchall()
        payload = []
        for r in rows:
            name = r[0]
            dob  = r[1]
            if dob:
                # Use current year so the date lands in the right calendar month
                bday_this_year = dob.replace(year=year) if hasattr(dob, "replace") else None
                date_str = str(bday_this_year) if bday_this_year else None
            else:
                date_str = None
            if date_str:
                payload.append({"name": name, "date": date_str})
        redis_json_setex_safe(ck, CACHE_TTL_DAY, payload)  # birthdays don't change mid-day
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try:
            if cursor: cursor.close()
            if conn:   _emp_pool_return(conn)
        except Exception:
            pass


# ── Calendar alias routes (HTML calls /api/calendar/...) ──────

@app.get("/api/calendar/festivals")
async def calendar_festivals_alias(year: int, month: int):
    """Alias so the HTML calendar can call /api/calendar/festivals."""
    return await get_festivals(year=year, month=month)


@app.get("/api/calendar/birthdays")
async def calendar_birthdays_alias(year: int, month: int):
    """Alias so the HTML calendar can call /api/calendar/birthdays."""
    return await get_birthdays(year=year, month=month)


# ══════════════════════════════════════════════════════════════
# ROUTES — REPORTEES & APPRAISAL HIERARCHY
# ══════════════════════════════════════════════════════════════

@app.get("/api/appraisal/reportees/{employee_id}")
def get_reportees_for_appraisal(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_reportees", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        reportees = _db_get_direct_reportees(conn, employee_id)
        payload = {"success": True, "reportees": reportees}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


@app.get("/api/appraisal/hierarchy/{employee_id}")
def get_appraisal_hierarchy(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_hierarchy", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, d.DEPT_NAME, jw.MANAGER_ID
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            WHERE e.STATUS = 'ACTIVE'
            ORDER BY e.EMPLOYEE_NAME
            """
        )
        all_employees = cursor.fetchall()
        cursor.close()

        supervisors = []
        reportees   = []
        current_emp = next((e for e in all_employees if e["EMPLOYEE_ID"] == clean_id), None)

        if current_emp:
            level = 1
            manager_id = current_emp.get("MANAGER_ID")
            while manager_id:
                manager = next((e for e in all_employees if e["EMPLOYEE_ID"] == manager_id), None)
                if manager:
                    supervisors.append({"EMPLOYEE_ID": manager["EMPLOYEE_ID"], "EMPLOYEE_NAME": manager["EMPLOYEE_NAME"], "LEVEL": level})
                    manager_id = manager.get("MANAGER_ID")
                    level += 1
                else:
                    break

            reportees = [
                {"EMPLOYEE_ID": e["EMPLOYEE_ID"], "EMPLOYEE_NAME": e["EMPLOYEE_NAME"], "DEPT_NAME": e["DEPT_NAME"], "LEVEL": 1}
                for e in all_employees
                if e.get("MANAGER_ID") == clean_id
            ]

        payload = {"success": True, "supervisors": supervisors, "reportees": reportees}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


@app.get("/api/appraisal/viewable/{employee_id}")
def get_viewable_appraisals(employee_id: str):
    clean_id = employee_id.strip().upper()
    ck = cache_key("appraisal_viewable", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            WITH LatestAppraisal AS (
                SELECT EMPLOYEE_ID, FINAL_RATING, COMMENTS, APPRAISAL_DATE, SUBMITTED_BY,
                       ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_ID ORDER BY APPRAISAL_DATE DESC) rn
                FROM EMP_NRM_APPRAISAL_SUMMARY
            )
            SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, d.DEPT_NAME,
                   la.FINAL_RATING, la.APPRAISAL_DATE, la.SUBMITTED_BY
            FROM EMP_NRM_JOB_WORK jw
            JOIN EMP_NRM_EMPLOYEES e ON jw.EMPLOYEE_ID = e.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN LatestAppraisal la ON e.EMPLOYEE_ID = la.EMPLOYEE_ID AND la.rn = 1
            WHERE jw.MANAGER_ID = %s AND e.STATUS = 'ACTIVE'
            ORDER BY e.EMPLOYEE_NAME
            """,
            (clean_id,),
        )
        team_appraisals = cursor.fetchall()
        cursor.close()
        payload = {"success": True, "viewable_appraisals": team_appraisals}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))
    finally:
        try: _emp_pool_return(conn)
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# ROUTES — EMPLOYEE HIERARCHY (org chart)
# ══════════════════════════════════════════════════════════════

@app.get("/api/employee/hierarchy/supervisors/{employee_id}")
async def route_get_supervisor_chain(employee_id: str):
    """Complete supervisor chain from employee up to CEO."""
    clean_id = (employee_id or "").strip().upper()
    ck = cache_key("hierarchy_supervisors", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "Database connection failed")

        cursor = conn.cursor(DictCursor)
        query = """
        WITH RECURSIVE supervisor_chain AS (
            SELECT jw.EMPLOYEE_ID, jw.MANAGER_ID,
                   e.EMPLOYEE_NAME,
                   des.TITLE AS DESIGNATION, dept.DEPT_NAME AS DEPARTMENT, 1 AS LEVEL
            FROM EMP_NRM_JOB_WORK jw
            LEFT JOIN EMP_NRM_EMPLOYEES e   ON jw.MANAGER_ID = e.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des  ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS  dept ON jw.DEPT_ID = dept.DEPT_ID
            WHERE jw.EMPLOYEE_ID = %s AND jw.MANAGER_ID IS NOT NULL

            UNION ALL

            SELECT jw2.EMPLOYEE_ID, jw2.MANAGER_ID,
                   e2.EMPLOYEE_NAME,
                   des2.TITLE AS DESIGNATION, dept2.DEPT_NAME AS DEPARTMENT, sc.LEVEL + 1
            FROM EMP_NRM_JOB_WORK jw2
            INNER JOIN supervisor_chain sc ON jw2.EMPLOYEE_ID = sc.MANAGER_ID
            LEFT JOIN EMP_NRM_EMPLOYEES e2   ON jw2.MANAGER_ID = e2.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des2  ON jw2.DESIGNATION_ID = des2.DESIGNATION_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS  dept2 ON jw2.DEPT_ID = dept2.DEPT_ID
            WHERE jw2.MANAGER_ID IS NOT NULL AND sc.LEVEL < 10
        )
        SELECT MANAGER_ID AS employee_id, EMPLOYEE_NAME AS name,
               DESIGNATION AS designation, DEPARTMENT AS department, LEVEL AS level
        FROM supervisor_chain
        ORDER BY LEVEL ASC
        """
        cursor.execute(query, (clean_id,))
        supervisors = cursor.fetchall()
        cursor.close()

        chain = [
            {"employee_id": s["employee_id"], "name": s["name"], "designation": s["designation"], "department": s["department"], "level": s["level"]}
            for s in supervisors
        ]
        payload = {"success": True, "employee_id": clean_id, "chain": chain, "chain_length": len(chain)}
        redis_json_setex_safe(ck, CACHE_TTL_MEDIUM, payload)
        return payload

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Failed to fetch supervisor chain: {str(e)}")
    finally:
        if conn:
            try: _emp_pool_return(conn)
            except Exception: pass


@app.get("/api/employee/hierarchy/reportees/{employee_id}")
async def route_get_reportees(employee_id: str, direct_only: bool = False):
    """Direct or full reportee tree for an employee."""
    clean_id = (employee_id or "").strip().upper()
    ck = cache_key("hierarchy_reportees", clean_id, "direct" if direct_only else "full")
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "Database connection failed")

        cursor = conn.cursor(DictCursor)

        if direct_only:
            cursor.execute(
                """
                SELECT jw.EMPLOYEE_ID AS employee_id, e.EMPLOYEE_NAME AS name,
                       des.TITLE AS designation, dept.DEPT_NAME AS department, e.EMAIL AS email,
                       (SELECT COUNT(*) FROM EMP_NRM_JOB_WORK jw_s WHERE jw_s.MANAGER_ID = jw.EMPLOYEE_ID) AS total_reportees
                FROM EMP_NRM_JOB_WORK jw
                LEFT JOIN EMP_NRM_EMPLOYEES    e    ON jw.EMPLOYEE_ID   = e.EMPLOYEE_ID
                LEFT JOIN EMP_NRM_DESIGNATIONS des  ON jw.DESIGNATION_ID = des.DESIGNATION_ID
                LEFT JOIN EMP_NRM_DEPARTMENTS  dept ON jw.DEPT_ID        = dept.DEPT_ID
                WHERE jw.MANAGER_ID = %s
                ORDER BY e.EMPLOYEE_NAME
                """,
                (clean_id,),
            )
            direct = cursor.fetchall()
            cursor.close()
            reportees = [
                {"employee_id": r["employee_id"], "name": r["name"], "designation": r["designation"],
                 "department": r["department"], "email": r["email"], "total_reportees": r["total_reportees"]}
                for r in direct
            ]
            payload = {
                "success": True, "employee_id": clean_id, "reportees": reportees,
                "stats": {"direct_reportees": len(reportees), "total_reportees": len(reportees)},
            }
            redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
            return payload

        # Full tree — build recursively via Python (cursor stays open throughout)
        def _build_tree(manager_id: str, level: int = 0, max_depth: int = 10) -> List[Dict]:
            if level >= max_depth:
                return []
            cursor.execute(
                """
                SELECT jw.EMPLOYEE_ID AS employee_id, e.EMPLOYEE_NAME AS name,
                       des.TITLE AS designation, dept.DEPT_NAME AS department, e.EMAIL AS email
                FROM EMP_NRM_JOB_WORK jw
                LEFT JOIN EMP_NRM_EMPLOYEES    e    ON jw.EMPLOYEE_ID   = e.EMPLOYEE_ID
                LEFT JOIN EMP_NRM_DESIGNATIONS des  ON jw.DESIGNATION_ID = des.DESIGNATION_ID
                LEFT JOIN EMP_NRM_DEPARTMENTS  dept ON jw.DEPT_ID        = dept.DEPT_ID
                WHERE jw.MANAGER_ID = %s
                ORDER BY e.EMPLOYEE_NAME
                """,
                (manager_id,),
            )
            rows = cursor.fetchall()
            result = []
            for r in rows:
                node = {
                    "employee_id": r["employee_id"], "name": r["name"],
                    "designation": r["designation"], "department": r["department"],
                    "email": r["email"], "level": level + 1,
                    "reportees": _build_tree(r["employee_id"], level + 1, max_depth),
                }
                result.append(node)
            return result

        tree = _build_tree(clean_id)
        cursor.close()

        def _count_total(nodes: List[Dict]) -> int:
            return sum(1 + _count_total(n.get("reportees", [])) for n in nodes)

        def _max_depth(nodes: List[Dict], d: int = 0) -> int:
            if not nodes:
                return d
            return max(_max_depth(n.get("reportees", []), d + 1) for n in nodes)

        payload = {
            "success": True, "employee_id": clean_id, "reportees": tree,
            "stats": {
                "direct_reportees": len(tree),
                "total_reportees": _count_total(tree),
                "hierarchy_level": _max_depth(tree),
            },
        }
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Failed to fetch reportees: {str(e)}")
    finally:
        if conn:
            try: _emp_pool_return(conn)
            except Exception: pass


@app.get("/api/employee/hierarchy/full/{employee_id}")
async def get_full_hierarchy(employee_id: str):
    """Combined supervisor chain + full reportee tree for an employee."""
    clean_id = (employee_id or "").strip().upper()
    ck = cache_key("hierarchy_full", clean_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    try:
        # Call route handlers directly as coroutines (avoids HTTP round-trip)
        supervisor_response   = await route_get_supervisor_chain(clean_id)
        direct_response       = await route_get_reportees(clean_id, direct_only=True)
        full_tree_response    = await route_get_reportees(clean_id, direct_only=False)

        payload = {
            "success": True,
            "employee_id": clean_id,
            "supervisor_chain":    supervisor_response.get("chain", []),
            "direct_reportees":    direct_response.get("reportees", []),
            "full_reportee_tree":  full_tree_response.get("reportees", []),
            "stats": {
                "levels_above":        supervisor_response.get("chain_length", 0),
                "direct_reportees":    direct_response.get("stats", {}).get("direct_reportees", 0),
                "total_reportees":     full_tree_response.get("stats", {}).get("total_reportees", 0),
                "max_depth_below":     full_tree_response.get("stats", {}).get("hierarchy_level", 0),
            },
        }
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Failed to fetch full hierarchy: {str(e)}")


@app.get("/api/employee/hierarchy/org-chart")
async def get_org_chart():
    """Full org chart starting from the top-level manager (CEO)."""
    ck = cache_key("hierarchy_org_chart")
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "Database connection failed")

        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT jw.EMPLOYEE_ID AS employee_id, e.EMPLOYEE_NAME AS name,
                   des.TITLE AS designation, dept.DEPT_NAME AS department
            FROM EMP_NRM_JOB_WORK jw
            LEFT JOIN EMP_NRM_EMPLOYEES    e    ON jw.EMPLOYEE_ID   = e.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des  ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS  dept ON jw.DEPT_ID        = dept.DEPT_ID
            WHERE jw.MANAGER_ID IS NULL OR jw.MANAGER_ID = ''
            LIMIT 1
            """
        )
        ceo = cursor.fetchone()
        cursor.close()
        _emp_pool_return(conn)
        conn = None

        if not ceo:
            return {"success": False, "message": "CEO not found in organization structure"}

        org_tree = await route_get_reportees(ceo["employee_id"], direct_only=False)
        payload = {
            "success": True,
            "ceo": {"employee_id": ceo["employee_id"], "name": ceo["name"], "designation": ceo["designation"], "department": ceo["department"]},
            "organization_tree": org_tree.get("reportees", []),
            "stats": org_tree.get("stats", {}),
        }
        redis_json_setex_safe(ck, CACHE_TTL_MEDIUM, payload)
        return payload

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Failed to fetch org chart: {str(e)}")
    finally:
        if conn:
            try: _emp_pool_return(conn)
            except Exception: pass


# ══════════════════════════════════════════════════════════════
# ROUTES — EMPLOYEE SEARCH
# ══════════════════════════════════════════════════════════════

@app.get("/api/employee/search")
async def search_employees(request: Request, query: str, limit: int = 10):
    clean_query = (query or "").strip()
    safe_limit  = max(1, min(limit, 50))
    ck = cache_key("employee_search", clean_query.lower(), safe_limit)

    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    client_id = get_client_identifier(request, clean_query or "SEARCH")
    rate_limit_key = cache_key("rate_employee_search", client_id)
    if not redis_rate_limit_allow(rate_limit_key, limit=120, window_seconds=60):
        raise HTTPException(429, "Too many search requests. Please retry after a minute")

    print(f"🟨 Redis MISS: {ck}")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "Database connection failed")

        cursor = conn.cursor(DictCursor)
        search_pattern = f"%{clean_query}%"
        cursor.execute(
            """
            SELECT e.EMPLOYEE_ID AS employee_id, e.EMPLOYEE_NAME AS name, e.EMAIL AS email,
                   des.TITLE AS designation, dept.DEPT_NAME AS department, jw.MANAGER_ID AS manager_id
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw  ON e.EMPLOYEE_ID   = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des  ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS  dept ON jw.DEPT_ID        = dept.DEPT_ID
            WHERE LOWER(e.EMPLOYEE_NAME) LIKE LOWER(%s)
               OR LOWER(e.EMPLOYEE_ID)   LIKE LOWER(%s)
               OR LOWER(e.EMAIL)         LIKE LOWER(%s)
            LIMIT %s
            """,
            (search_pattern, search_pattern, search_pattern, safe_limit),
        )
        results = cursor.fetchall()
        cursor.close()

        employees = [
            {"employee_id": emp["employee_id"], "name": emp["name"], "email": emp["email"],
             "designation": emp["designation"], "department": emp["department"], "manager_id": emp["manager_id"]}
            for emp in results
        ]
        payload = {"success": True, "query": clean_query, "results": employees, "count": len(employees)}
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, payload)
        return payload

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Failed to search employees: {str(e)}")
    finally:
        if conn:
            try: _emp_pool_return(conn)
            except Exception: pass


# ══════════════════════════════════════════════════════════════
# ROUTES — MANAGER LEAVE MANAGEMENT
# ══════════════════════════════════════════════════════════════

@app.get("/api/manager/pending-leaves")
def get_pending_leaves(manager_id: str):
    clean_manager_id = (manager_id or "").strip().upper()
    ck = cache_key("manager_pending_leaves", clean_manager_id)
    cached = redis_json_get_safe(ck)
    if cached is not None:
        print(f"✅ Redis HIT: {ck}")
        return cached

    print(f"🟨 Redis MISS: {ck}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT l.LEAVE_ID, l.EMPLOYEE_ID, e.EMPLOYEE_NAME,
                   l.START_DATE, l.END_DATE, l.REASON, l.APPLIED_AT,
                   DATEDIFF(day, l.START_DATE, l.END_DATE) + 1 AS DURATION
            FROM EMP_NRM_LEAVE l
            JOIN EMP_NRM_EMPLOYEES e ON l.EMPLOYEE_ID = e.EMPLOYEE_ID
            JOIN EMP_NRM_JOB_WORK jw ON l.EMPLOYEE_ID = jw.EMPLOYEE_ID
            WHERE jw.MANAGER_ID = %s AND l.STATUS = 'Pending'
            ORDER BY l.APPLIED_AT DESC
            """,
            (clean_manager_id,),
        )
        rows = cursor.fetchall()
        redis_json_setex_safe(ck, CACHE_TTL_SHORT, rows)
        return rows
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: cursor.close()
        except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.get("/manager/leave-action")   # alias – nginx may route /manager/* directly here
@app.get("/api/manager/leave-action")
def leave_action_via_email(token: str):
    """One-click approve/reject from email link. Validates HMAC token, updates DB, busts cache,
    and notifies the employee of the decision."""
    try:
        leave_id, action = _verify_leave_token(token)
    except ValueError as e:
        raise HTTPException(400, f"Invalid or tampered link: {e}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT l.EMPLOYEE_ID, jw.MANAGER_ID, l.STATUS,
                   l.LEAVE_TYPE, l.START_DATE, l.END_DATE, l.REASON,
                   e.EMPLOYEE_NAME, e.EMAIL AS EMP_EMAIL
            FROM EMP_NRM_LEAVE l
            LEFT JOIN EMP_NRM_EMPLOYEES e ON l.EMPLOYEE_ID = e.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_JOB_WORK jw ON l.EMPLOYEE_ID = jw.EMPLOYEE_ID
            WHERE l.LEAVE_ID = %s LIMIT 1
            """,
            (leave_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Leave record not found")

        current_status = row[2]
        if current_status != "Pending":
            from fastapi.responses import HTMLResponse
            return HTMLResponse(
                content=f"<h2>Already {current_status}</h2><p>Leave #{leave_id} was already {current_status.lower()}.</p>",
                status_code=200,
            )

        new_status = "Approved" if action == "approve" else "Rejected"
        cursor.execute(
            "UPDATE EMP_NRM_LEAVE SET STATUS = %s WHERE LEAVE_ID = %s",
            (new_status, leave_id),
        )
        conn.commit()

        employee_id   = (str(row[0]) or "").strip().upper()
        manager_id    = (str(row[1]) or "").strip().upper() if row[1] else None
        leave_type    = row[3] or ""
        start_date    = row[4]
        end_date      = row[5]
        reason        = row[6] or ""
        employee_name = row[7] or employee_id
        emp_email     = row[8] or ""

        invalidate_leave(employee_id, manager_id)

        # Notify employee of the decision
        if emp_email:
            def _notify_employee(_name=employee_name, _email=emp_email, _lid=leave_id,
                                 _lt=leave_type, _sd=start_date, _ed=end_date,
                                 _status=new_status):
                try:
                    sender = _admin_email()
                    icon   = "✅" if _status == "Approved" else "❌"
                    color  = "#22c55e" if _status == "Approved" else "#ef4444"
                    note   = ("Your leave has been approved. Enjoy your time off!" if _status == "Approved"
                              else "Your leave request was not approved. Please speak with your manager for more details.")
                    rows = (
                        _row("🔖", "Leave ID",   str(_lid)) +
                        _row("📋", "Leave Type",  _lt or "—") +
                        _row("📅", "From",        str(_sd)) +
                        _row("📅", "To",          str(_ed)) +
                        _row("📊", "Status",      f'<span style="color:{color};font-weight:700;">{icon} {_status}</span>')
                    )
                    html = _email_html(
                        icon=icon,
                        heading=f"Leave {_status}",
                        subheading=f"Your leave request has been {_status.lower()}",
                        rows_html=rows,
                        footer_note=note + " ",
                    )
                    text = (
                        f"Hi {_name},\n\n"
                        f"Your leave request (ID: {_lid}) has been {_status.lower()}.\n\n"
                        f"Type  : {_lt}\nFrom  : {_sd}  To: {_ed}\n\n"
                        f"{note}\n\nRegards,\nChakoraHub Team"
                    )
                    _ses_client().send_email(
                        Source=sender,
                        Destination={"ToAddresses": [_email]},
                        Message={
                            "Subject": {"Data": f"Leave {_status} – {_lt or 'Leave'} ({_sd} to {_ed})"},
                            "Body": {"Html": {"Data": html}, "Text": {"Data": text}},
                        },
                    )
                    print(f"✅ Leave {_status.lower()} email sent to employee: {_email}")
                except Exception as ex:
                    print(f"❌ Leave status email error (non-fatal): {ex}")
            threading.Thread(target=_notify_employee, daemon=True).start()

        from fastapi.responses import HTMLResponse
        color = "#22c55e" if new_status == "Approved" else "#ef4444"
        return HTMLResponse(content=f"""
<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>Leave {new_status} – ChakoraHub</title></head>
<body style="font-family:Arial,sans-serif;display:flex;align-items:center;justify-content:center;
             min-height:100vh;margin:0;background:#eef2f7;">
  <div style="text-align:center;background:#fff;border-radius:12px;padding:48px 40px;
              box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:400px;">
    <div style="font-size:56px;margin-bottom:16px;">{"✅" if new_status == "Approved" else "❌"}</div>
    <h2 style="color:{color};margin:0 0 12px;">Leave {new_status}</h2>
    <p style="color:#4a5568;margin:0;">Leave request #{leave_id} has been <strong>{new_status.lower()}</strong> successfully.
    {"The employee has been notified." if emp_email else ""}</p>
  </div>
</body></html>""", status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: cursor.close()
        except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass



# ══════════════════════════════════════════════════════════════
# 360 DASHBOARD JSON ENDPOINTS  (mobile — employee_service port 8002)
# ══════════════════════════════════════════════════════════════

@app.get("/api/employee/elearn360/{employee_id}")
async def get_elearn360(employee_id: str):
    """ELearn 360: skills, certs, appraisal skills, courses."""
    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn: raise HTTPException(500, "Database connection failed")
        cursor = conn.cursor(DictCursor)

        cursor.execute("SELECT NAME, LEVEL, CERTIFIED, UPDATED FROM EMP_NRM_SKILLS WHERE EMPLOYEE_ID = %s", (employee_id,))
        skills = [dict(r) for r in (cursor.fetchall() or [])]

        cursor.execute("SELECT DOCUMENT_TYPE, FILE_PATH, UPLOAD_DATE FROM EMP_NRM_DOCUMENTS WHERE EMPLOYEE_ID = %s ORDER BY UPLOAD_DATE DESC", (employee_id,))
        certs = [dict(r) for r in (cursor.fetchall() or [])]

        cursor.execute("""SELECT SKILL_NAME AS name, PROFICIENCY_LEVEL AS level, YEARS_OF_EXPERIENCE
            FROM EMP_NRM_APPRAISAL_SKILLS WHERE EMPLOYEE_ID = %s""", (employee_id,))
        level_pct = {'beginner':25,'intermediate':50,'advanced':75,'expert':95}
        raw = cursor.fetchall() or []
        appraisal_skills = []
        for r in raw:
            lv = (r.get('level') or r.get('LEVEL') or 'beginner').lower()
            appraisal_skills.append({'name': r.get('name') or r.get('NAME') or '', 'level': lv.capitalize(), 'pct': level_pct.get(lv, 25)})

        cursor.execute("""SELECT c.COURSE_NAME AS subject, COUNT(v.ID) AS video_count, MIN(v.YOUTUBE_ID) AS first_yt_id
            FROM NRM_COURSES c LEFT JOIN nrm_video_sessions v ON v.COURSE_ID = c.ID
            GROUP BY c.ID, c.COURSE_NAME ORDER BY c.COURSE_NAME""")
        raw_courses = cursor.fetchall() or []
        courses = []
        for r in raw_courses:
            yt = r.get('FIRST_YT_ID') or r.get('first_yt_id') or ''
            courses.append({'subject': r.get('SUBJECT') or r.get('subject') or '', 'video_count': r.get('VIDEO_COUNT') or r.get('video_count') or 0, 'first_url': f'https://www.youtube.com/watch?v={yt}' if yt else ''})

        return {"success": True, "skills": skills, "certs": certs, "appraisal_skills": appraisal_skills, "courses": courses}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))
    finally:
        if cursor: cursor.close()
        if conn: _emp_pool_return(conn)


@app.get("/api/employee/infra360/{employee_id}")
async def get_infra360(employee_id: str):
    """Infra 360: department, designation, active asset."""
    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn: raise HTTPException(500, "Database connection failed")
        cursor = conn.cursor(DictCursor)

        cursor.execute("""SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, d.DEPT_NAME, des.TITLE AS DESIGNATION
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE e.EMPLOYEE_ID = %s LIMIT 1""", (employee_id,))
        emp = cursor.fetchone()

        cursor.execute("""SELECT ASSET_TYPE, ASSET_TAG, ALLOCATED_DATE, STATUS
            FROM EMP_NRM_ASSET_ALLOCATION
            WHERE EMPLOYEE_ID = %s AND STATUS = 'Active'
            ORDER BY ALLOCATED_DATE DESC LIMIT 1""", (employee_id,))
        asset = cursor.fetchone()

        alloc_date = 'N/A'
        if asset and asset.get('ALLOCATED_DATE'):
            try: alloc_date = asset['ALLOCATED_DATE'].strftime('%Y-%m-%d')
            except: alloc_date = str(asset['ALLOCATED_DATE'])

        return {"success": True,
            "emp_name":    (emp or {}).get('EMPLOYEE_NAME') or employee_id,
            "emp_id":      (emp or {}).get('EMPLOYEE_ID')   or employee_id,
            "department":  (emp or {}).get('DEPT_NAME')     or 'N/A',
            "designation": (emp or {}).get('DESIGNATION')   or 'N/A',
            "asset_name":  (asset or {}).get('ASSET_TYPE')  or 'No Asset Assigned',
            "asset_type":  (asset or {}).get('ASSET_TYPE')  or 'N/A',
            "serial_number": (asset or {}).get('ASSET_TAG') or 'N/A',
            "allocation_date": alloc_date,
            "status":      (asset or {}).get('STATUS')      or 'N/A'}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))
    finally:
        if cursor: cursor.close()
        if conn: _emp_pool_return(conn)


@app.get("/api/employee/personal360/{employee_id}")
async def get_personal360(employee_id: str):
    """Personal 360: employee record, profile, job, leaves, assets, appraisal."""
    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn: raise HTTPException(500, "Database connection failed")
        cursor = conn.cursor(DictCursor)

        personal: Dict[str, Any] = {'employee': {}, 'profile': {}, 'job': {}, 'leaves': [], 'assets': [], 'appraisal': []}

        cursor.execute("SELECT EMPLOYEE_ID, EMPLOYEE_NAME, STATUS, EMAIL FROM EMP_NRM_EMPLOYEES WHERE EMPLOYEE_ID = %s LIMIT 1", (employee_id,))
        r = cursor.fetchone()
        if r: personal['employee'] = dict(r)

        cursor.execute("SELECT CONTACT, ADDRESS, BANK_DETAILS FROM EMP_NRM_PROFILE WHERE EMP_ID = %s LIMIT 1", (employee_id,))
        r = cursor.fetchone()
        if r: personal['profile'] = dict(r)

        cursor.execute("""SELECT jw.DEPT_ID, d.DEPT_NAME, des.TITLE AS DESIGNATION, jw.LOCATION_ID, jw.CREATED_AT
            FROM EMP_NRM_JOB_WORK jw
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE jw.EMPLOYEE_ID = %s LIMIT 1""", (employee_id,))
        r = cursor.fetchone()
        if r:
            personal['job'] = dict(r)
            personal['profile']['LOCATION'] = r.get('LOCATION_ID') or ''

        cursor.execute("SELECT LEAVE_TYPE, START_DATE, END_DATE, TOTAL_DAYS, STATUS FROM EMP_NRM_LEAVE WHERE EMPLOYEE_ID = %s ORDER BY APPLIED_AT DESC LIMIT 20", (employee_id,))
        personal['leaves'] = [dict(row) for row in (cursor.fetchall() or [])]

        cursor.execute("SELECT ASSET_TYPE, ASSIGNED_DATE, RETURN_DATE, STATUS FROM EMP_NRM_ASSETS WHERE EMP_ID = %s", (employee_id,))
        personal['assets'] = [dict(row) for row in (cursor.fetchall() or [])]

        cursor.execute("SELECT FINAL_RATING, COMMENTS, APPRAISAL_DATE, SUBMITTED_BY FROM EMP_NRM_APPRAISAL_SUMMARY WHERE EMPLOYEE_ID = %s ORDER BY APPRAISAL_DATE DESC LIMIT 5", (employee_id,))
        personal['appraisal'] = [dict(row) for row in (cursor.fetchall() or [])]

        # Serialize dates to strings
        import json, datetime
        def _serialize(obj):
            if isinstance(obj, (datetime.date, datetime.datetime)): return str(obj)
            raise TypeError
        return json.loads(json.dumps({"success": True, "personal": personal}, default=_serialize))
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))
    finally:
        if cursor: cursor.close()
        if conn: _emp_pool_return(conn)


@app.post("/api/manager/approve-leave/{leave_id}")
def approve_leave(leave_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT l.EMPLOYEE_ID, jw.MANAGER_ID,
                   l.LEAVE_TYPE, l.START_DATE, l.END_DATE,
                   e.EMPLOYEE_NAME, e.EMAIL AS EMP_EMAIL
            FROM EMP_NRM_LEAVE l
            LEFT JOIN EMP_NRM_JOB_WORK jw ON l.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_EMPLOYEES e  ON l.EMPLOYEE_ID = e.EMPLOYEE_ID
            WHERE l.LEAVE_ID = %s LIMIT 1
            """,
            (leave_id,),
        )
        leave_row = cursor.fetchone()

        cursor.execute("UPDATE EMP_NRM_LEAVE SET STATUS = 'Approved' WHERE LEAVE_ID = %s", (leave_id,))
        conn.commit()

        if leave_row:
            employee_id   = (str(leave_row['EMPLOYEE_ID']) or "").strip().upper()
            manager_id    = (str(leave_row['MANAGER_ID'])  or "").strip().upper() if leave_row.get('MANAGER_ID') else None
            emp_email     = leave_row.get('EMP_EMAIL') or ""
            employee_name = leave_row.get('EMPLOYEE_NAME') or employee_id
            leave_type    = leave_row.get('LEAVE_TYPE') or ""
            start_date    = leave_row.get('START_DATE')
            end_date      = leave_row.get('END_DATE')
            invalidate_leave(employee_id, manager_id)
            if emp_email:
                def _notify(_name=employee_name, _email=emp_email, _lid=leave_id,
                            _lt=leave_type, _sd=start_date, _ed=end_date, _status="Approved"):
                    try:
                        sender = _admin_email()
                        rows = (
                            _row("🔖", "Leave ID",   str(_lid)) +
                            _row("📋", "Leave Type",  _lt or "—") +
                            _row("📅", "From",        str(_sd)) +
                            _row("📅", "To",          str(_ed)) +
                            _row("📊", "Status",      '<span style="color:#22c55e;font-weight:700;">✅ Approved</span>')
                        )
                        html = _email_html(icon="✅", heading="Leave Approved",
                            subheading="Your leave request has been approved",
                            rows_html=rows,
                            footer_note="Your leave has been approved. Enjoy your time off! ")
                        text = (f"Hi {_name},\n\nYour leave (ID: {_lid}) has been Approved.\n"
                                f"Type: {_lt}  From: {_sd}  To: {_ed}\n\nEnjoy your time off!\n\nRegards,\nChakoraHub Team")
                        _ses_client().send_email(
                            Source=sender,
                            Destination={"ToAddresses": [_email]},
                            Message={
                                "Subject": {"Data": f"Leave Approved – {_lt or 'Leave'} ({_sd} to {_ed})"},
                                "Body": {"Html": {"Data": html}, "Text": {"Data": text}},
                            })
                        print(f"✅ Leave Approved email sent to: {_email}")
                    except Exception as ex:
                        print(f"⚠️ Leave status email error (non-fatal): {ex}")
                threading.Thread(target=_notify, daemon=True).start()

        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: cursor.close()
        except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass


@app.post("/api/manager/reject-leave/{leave_id}")
def reject_leave(leave_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT l.EMPLOYEE_ID, jw.MANAGER_ID,
                   l.LEAVE_TYPE, l.START_DATE, l.END_DATE,
                   e.EMPLOYEE_NAME, e.EMAIL AS EMP_EMAIL
            FROM EMP_NRM_LEAVE l
            LEFT JOIN EMP_NRM_JOB_WORK jw ON l.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_EMPLOYEES e  ON l.EMPLOYEE_ID = e.EMPLOYEE_ID
            WHERE l.LEAVE_ID = %s LIMIT 1
            """,
            (leave_id,),
        )
        leave_row = cursor.fetchone()

        cursor.execute("UPDATE EMP_NRM_LEAVE SET STATUS = 'Rejected' WHERE LEAVE_ID = %s", (leave_id,))
        conn.commit()

        if leave_row:
            employee_id   = (str(leave_row['EMPLOYEE_ID']) or "").strip().upper()
            manager_id    = (str(leave_row['MANAGER_ID'])  or "").strip().upper() if leave_row.get('MANAGER_ID') else None
            emp_email     = leave_row.get('EMP_EMAIL') or ""
            employee_name = leave_row.get('EMPLOYEE_NAME') or employee_id
            leave_type    = leave_row.get('LEAVE_TYPE') or ""
            start_date    = leave_row.get('START_DATE')
            end_date      = leave_row.get('END_DATE')
            invalidate_leave(employee_id, manager_id)
            if emp_email:
                def _notify(_name=employee_name, _email=emp_email, _lid=leave_id,
                            _lt=leave_type, _sd=start_date, _ed=end_date, _status="Rejected"):
                    try:
                        sender = _admin_email()
                        rows = (
                            _row("🔖", "Leave ID",   str(_lid)) +
                            _row("📋", "Leave Type",  _lt or "—") +
                            _row("📅", "From",        str(_sd)) +
                            _row("📅", "To",          str(_ed)) +
                            _row("📊", "Status",      '<span style="color:#ef4444;font-weight:700;">❌ Rejected</span>')
                        )
                        html = _email_html(icon="❌", heading="Leave Rejected",
                            subheading="Your leave request was not approved",
                            rows_html=rows,
                            footer_note="Your leave request was not approved. Please speak with your manager for more details. ")
                        text = (f"Hi {_name},\n\nYour leave (ID: {_lid}) has been Rejected.\n"
                                f"Type: {_lt}  From: {_sd}  To: {_ed}\n\nPlease speak with your manager.\n\nRegards,\nChakoraHub Team")
                        _ses_client().send_email(
                            Source=sender,
                            Destination={"ToAddresses": [_email]},
                            Message={
                                "Subject": {"Data": f"Leave Rejected – {_lt or 'Leave'} ({_sd} to {_ed})"},
                                "Body": {"Html": {"Data": html}, "Text": {"Data": text}},
                            })
                        print(f"✅ Leave Rejected email sent to: {_email}")
                    except Exception as ex:
                        print(f"⚠️ Leave status email error (non-fatal): {ex}")
                threading.Thread(target=_notify, daemon=True).start()

        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try: cursor.close()
        except Exception: pass
        try: _emp_pool_return(conn)
        except Exception: pass

# ==========================================
# API ENDPOINTS
# ==========================================

@app.post("/asset/register", response_model=AssetResponse)
async def register_asset(request: AssetRegisterRequest):
    """
    Register a new asset or update existing asset
    
    Rules:
    1. Asset ID auto-generated with 'CH' prefix (e.g., CH-20250120-0001)
    2. Validates employee ID against EMP_NRM_EMPLOYEES table
    3. Purchase date cannot be in future
    4. Creates audit log for tracking
    5. Handles constraint violations gracefully
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor(DictCursor)
        resolved_model_id = resolve_model_id(conn, request.TYPE_ID, request.MODEL_ID)
        resolved_serial_id = resolve_serial_id(conn, resolved_model_id, request.SERIAL_ID)
        resolved_vendor_id = resolve_vendor_id(conn, request.VENDOR_ID)
        
        # Determine if this is CREATE or UPDATE
        is_update = bool(request.ASSET_ID and request.ASSET_ID.strip())
        
        if is_update:
            # ==========================================
            # UPDATE EXISTING ASSET
            # ==========================================
            asset_id = request.ASSET_ID.strip()
            
            # Check if asset exists
            cursor.execute("SELECT ASSET_ID FROM ASSETS WHERE ASSET_ID = %s", (asset_id,))
            existing = cursor.fetchone()
            
            if not existing:
                raise HTTPException(
                    status_code=404,
                    detail=f"Asset ID '{asset_id}' not found"
                )
            
            # Validate employee ID if provided
            if request.ASSIGNED_TO:
                is_valid, emp_name = validate_employee_id(conn, request.ASSIGNED_TO)
                if not is_valid:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Employee ID '{request.ASSIGNED_TO}' not found or inactive"
                    )
            
            # Update asset
            cursor.execute(
                """
                UPDATE ASSETS SET
                    TYPE_ID = %s,
                    MODEL_ID = %s,
                    SERIAL_ID = %s,
                    VENDOR_ID = %s,
                    OS = %s,
                    RAM = %s,
                    SSD = %s,
                    OWNED_BY = %s,
                    PURCHASE_DATE = %s,
                    WARRANTY_EXPIRY = %s,
                    LOCATION = %s,
                    CONDITION = %s,
                    STATUS = %s,
                    PRICE = %s,
                    ASSIGNED_TO = %s,
                    NOTES = %s
                WHERE ASSET_ID = %s
                """,
                (
                    request.TYPE_ID,
                    resolved_model_id,
                    resolved_serial_id,
                    resolved_vendor_id,
                    request.OS,
                    request.RAM,
                    request.SSD,
                    request.OWNED_BY,
                    request.PURCHASE_DATE,
                    request.WARRANTY_EXPIRY,
                    request.LOCATION,
                    request.CONDITION,
                    request.STATUS,
                    request.PRICE,
                    request.ASSIGNED_TO,
                    request.NOTES,
                    asset_id
                )
            )
            
            conn.commit()
            
            # Create audit log
            create_audit_log(
                conn, 
                asset_id, 
                "UPDATE", 
                request.ASSIGNED_TO or "SYSTEM",
                f"Asset updated - Model: {request.MODEL_ID}, Status: {request.STATUS}"
            )
            
            print(f"✅ Asset updated: {asset_id}")
            
            return AssetResponse(
                success=True,
                message=f"Asset {asset_id} updated successfully",
                asset_id=asset_id,
                asset_data=request.dict()
            )
        
        else:
            # ==========================================
            # CREATE NEW ASSET
            # ==========================================
            
            # Generate asset ID
            asset_id = generate_asset_id(conn)
            print(f"📝 Generated asset ID: {asset_id}")
            
            # Validate employee ID if provided
            if request.ASSIGNED_TO:
                is_valid, emp_name = validate_employee_id(conn, request.ASSIGNED_TO)
                if not is_valid:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Employee ID '{request.ASSIGNED_TO}' not found or inactive. Please enter a valid employee ID from the employee database."
                    )
                print(f"✅ Employee validated: {request.ASSIGNED_TO} - {emp_name}")
            
            # Insert new asset
            cursor.execute(
                """
                INSERT INTO ASSETS (
                    ASSET_ID, TYPE_ID, MODEL_ID, SERIAL_ID, VENDOR_ID,
                    OS, RAM, SSD, OWNED_BY, PURCHASE_DATE, WARRANTY_EXPIRY,
                    LOCATION, CONDITION, STATUS, PRICE, ASSIGNED_TO, NOTES
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    asset_id,
                    request.TYPE_ID,
                    resolved_model_id,
                    resolved_serial_id,
                    resolved_vendor_id,
                    request.OS,
                    request.RAM,
                    request.SSD,
                    request.OWNED_BY,
                    request.PURCHASE_DATE,
                    request.WARRANTY_EXPIRY,
                    request.LOCATION,
                    request.CONDITION,
                    request.STATUS,
                    request.PRICE,
                    request.ASSIGNED_TO,
                    request.NOTES
                )
            )
            
            conn.commit()
            
            # Create audit log
            create_audit_log(
                conn,
                asset_id,
                "CREATE",
                request.ASSIGNED_TO or "SYSTEM",
                f"New asset registered - {request.MODEL_ID}"
            )
            
            print(f"✅ Asset created: {asset_id}")
            
            return AssetResponse(
                success=True,
                message=f"Asset registered successfully with ID: {asset_id}",
                asset_id=asset_id,
                asset_data={**request.dict(), "ASSET_ID": asset_id}
            )
    
    except HTTPException:
        # Re-raise HTTP exceptions (validation errors)
        raise
    
    except Exception as e:
        print(f"❌ Asset registration error: {e}")
        traceback.print_exc()
        
        # Handle specific database constraint violations
        error_msg = str(e).lower()
        
        if "foreign key" in error_msg or "constraint" in error_msg:
            if "type_id" in error_msg:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid asset type selected. Please choose a valid asset type."
                )
            elif "employee" in error_msg or "assigned_to" in error_msg:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid employee ID. The employee does not exist or is inactive."
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Database constraint violation: {str(e)}"
                )
        
        elif "duplicate" in error_msg or "unique" in error_msg:
            raise HTTPException(
                status_code=409,
                detail="Asset with this ID or serial number already exists"
            )
        
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Asset registration failed: {str(e)}"
            )
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.get("/asset/{asset_id}")
async def get_asset(asset_id: str):
    """
    Get asset details by asset ID
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor(DictCursor)
        
        # Get asset with employee name if assigned
        cursor.execute(
            """
            SELECT 
                a.*,
                e.EMPLOYEE_NAME,
                at.TYPE_NAME as ASSET_TYPE_NAME
            FROM ASSETS a
            LEFT JOIN EMP_NRM_EMPLOYEES e ON a.ASSIGNED_TO = e.EMPLOYEE_ID
            LEFT JOIN ASSET_TYPES at ON a.TYPE_ID = at.ID
            WHERE a.ASSET_ID = %s
            """,
            (asset_id,)
        )
        
        asset = cursor.fetchone()
        
        if not asset:
            raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
        
        return {
            "success": True,
            "asset": asset
        }
    
    except HTTPException:
        raise
    
    except Exception as e:
        print(f"❌ Get asset error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.get("/assets/list")
async def list_assets(
    status: Optional[str] = None,
    assigned_to: Optional[str] = None,
    type_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    List all assets with optional filters
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor(DictCursor)
        
        # Build query with filters
        query = """
            SELECT 
                a.*,
                e.EMPLOYEE_NAME,
                at.TYPE_NAME as ASSET_TYPE_NAME
            FROM ASSETS a
            LEFT JOIN EMP_NRM_EMPLOYEES e ON a.ASSIGNED_TO = e.EMPLOYEE_ID
            LEFT JOIN ASSET_TYPES at ON a.TYPE_ID = at.ID
            WHERE 1=1
        """
        params = []
        
        if status:
            query += " AND a.STATUS = %s"
            params.append(status)
        
        if assigned_to:
            query += " AND a.ASSIGNED_TO = %s"
            params.append(assigned_to)
        
        if type_id:
            query += " AND a.TYPE_ID = %s"
            params.append(type_id)
        
        query += " ORDER BY a.ASSET_ID DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        assets = cursor.fetchall()
        
        return {
            "success": True,
            "count": len(assets),
            "assets": assets
        }
    
    except Exception as e:
        print(f"❌ List assets error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.delete("/asset/{asset_id}")
async def delete_asset(asset_id: str, actioned_by: str = "SYSTEM"):
    """
    Delete asset (or mark as retired)
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor(DictCursor)
        
        # Check if asset exists
        cursor.execute("SELECT ASSET_ID FROM ASSETS WHERE ASSET_ID = %s", (asset_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
        
        # Option 1: Soft delete (mark as Retired)
        cursor.execute(
            "UPDATE ASSETS SET STATUS = 'Retired' WHERE ASSET_ID = %s",
            (asset_id,)
        )
        
        # Option 2: Hard delete (uncomment if preferred)
        # cursor.execute("DELETE FROM ASSETS WHERE ASSET_ID = %s", (asset_id,))
        
        conn.commit()
        
        # Create audit log
        create_audit_log(conn, asset_id, "DELETE", actioned_by, "Asset retired/deleted")
        
        return {
            "success": True,
            "message": f"Asset {asset_id} retired successfully"
        }
    
    except HTTPException:
        raise
    
    except Exception as e:
        print(f"❌ Delete asset error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.get("/assets/employee/{employee_id}")
async def get_employee_assets(employee_id: str):
    """
    Get all assets assigned to a specific employee
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor(DictCursor)
        
        cursor.execute(
            """
            SELECT 
                a.*,
                e.EMPLOYEE_NAME,
                at.TYPE_NAME as ASSET_TYPE_NAME
            FROM ASSETS a
            LEFT JOIN EMP_NRM_EMPLOYEES e ON a.ASSIGNED_TO = e.EMPLOYEE_ID
            LEFT JOIN ASSET_TYPES at ON a.TYPE_ID = at.ID
            WHERE a.ASSIGNED_TO = %s
            ORDER BY a.PURCHASE_DATE DESC
            """,
            (employee_id,)
        )
        
        assets = cursor.fetchall()
        
        return {
            "success": True,
            "employee_id": employee_id,
            "asset_count": len(assets),
            "assets": assets
        }
    
    except Exception as e:
        print(f"❌ Get employee assets error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.get("/assets/stats")
async def get_asset_statistics():
    """
    Get asset statistics and summary
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor(DictCursor)
        
        stats = {}
        
        # Total assets
        cursor.execute("SELECT COUNT(*) as total FROM ASSETS")
        stats['total_assets'] = cursor.fetchone()['total']
        
        # By status
        cursor.execute("""
            SELECT STATUS, COUNT(*) as count 
            FROM ASSETS 
            GROUP BY STATUS
        """)
        stats['by_status'] = {row['STATUS']: row['count'] for row in cursor.fetchall()}
        
        # By type
        cursor.execute("""
            SELECT at.TYPE_NAME, COUNT(*) as count
            FROM ASSETS a
            LEFT JOIN ASSET_TYPES at ON a.TYPE_ID = at.ID
            GROUP BY at.TYPE_NAME
        """)
        stats['by_type'] = {row['TYPE_NAME']: row['count'] for row in cursor.fetchall()}
        
        # Assigned vs Unassigned
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN ASSIGNED_TO IS NOT NULL AND ASSIGNED_TO != '' THEN 1 ELSE 0 END) as assigned,
                SUM(CASE WHEN ASSIGNED_TO IS NULL OR ASSIGNED_TO = '' THEN 1 ELSE 0 END) as unassigned
            FROM ASSETS
        """)
        assignment_stats = cursor.fetchone()
        stats['assigned'] = assignment_stats['assigned']
        stats['unassigned'] = assignment_stats['unassigned']
        
        return {
            "success": True,
            "statistics": stats
        }
    
    except Exception as e:
        print(f"❌ Get asset stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)

# ══════════════════════════════════════════════════════════════
# ROUTES — SALARY & PAYSLIPS
# (consumed by the Flutter app's EmployeeApi.getSalary / getPayslips /
#  getPayslipDownloadUrl — lib/services/employee_api.dart)
# ══════════════════════════════════════════════════════════════

S3_BUCKET_EMPLOYEE = _get_env_value("S3_BUCKET_EMPLOYEE", "chakorahub-employee-docs")


def _employee_s3_client():
    access_key = _get_env_value("AWS_ACCESS_KEY") or _get_env_value("AWS_ACCESS_KEY_ID")
    secret_key = _get_env_value("AWS_SECRET_KEY") or _get_env_value("AWS_SECRET_ACCESS_KEY")
    region = _get_env_value("AWS_REGION", "eu-north-1")
    if access_key and secret_key:
        return boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    return boto3.client("s3", region_name=region)


@app.get("/api/employee/salary/{employee_id}")
async def get_employee_salary(employee_id: str):
    """
    Salary breakdown for one employee — basic/HRA/allowances/deductions/
    net_salary/gross_salary. Cached 2 min via DB 11 (salary:{employee_id})
    because payroll figures are sensitive and short-TTL by design.
    Returns {"salary": null} (not 404) when no EMP_NRM_SALARY row exists yet,
    so the Flutter "No salary record found" empty state can render cleanly.
    """
    cached = get_cached_salary(employee_id)
    if cached is not None:
        return {"success": True, "salary": cached, "cached": True}

    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT BASIC, HRA, ALLOWANCES, DEDUCTIONS, NET_SALARY
            FROM EMP_NRM_SALARY
            WHERE EMPLOYEE_ID = %s
            LIMIT 1
            """,
            (employee_id,),
        )
        row = cursor.fetchone()
        if not row:
            return {"success": True, "salary": None}

        basic = float(row.get("BASIC") or 0)
        hra = float(row.get("HRA") or 0)
        allowances = float(row.get("ALLOWANCES") or 0)
        deductions = float(row.get("DEDUCTIONS") or 0)
        net_salary = float(row.get("NET_SALARY") or 0)

        salary = {
            "basic": basic,
            "hra": hra,
            "allowances": allowances,
            "deductions": deductions,
            "net_salary": net_salary,
            "gross_salary": basic + hra + allowances,
        }
        cache_salary(employee_id, salary)
        return {"success": True, "salary": salary}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Get salary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.get("/api/employee/payslips/{employee_id}")
async def get_employee_payslips(employee_id: str):
    """List of generated payslips for one employee (metadata only — no
    download link; call /api/employee/payslip/download/{slip_id} for that)."""
    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT SLIP_ID, MONTH, YEAR, GENERATED_AT
            FROM EMP_NRM_SALARY_SLIPS
            WHERE EMP_ID = %s
            ORDER BY YEAR DESC, MONTH DESC
            """,
            (employee_id,),
        )
        rows = cursor.fetchall() or []
        slips = [
            {
                "slip_id": r.get("SLIP_ID"),
                "month": r.get("MONTH"),
                "year": r.get("YEAR"),
                "generated_at": str(r.get("GENERATED_AT")) if r.get("GENERATED_AT") else None,
            }
            for r in rows
        ]
        return {"success": True, "payslips": slips}
    except Exception as e:
        print(f"❌ Get payslips error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


@app.get("/api/employee/payslip/download/{slip_id}")
async def get_employee_payslip_download(slip_id: int, employee_id: str):
    """5-minute pre-signed S3 URL for one payslip PDF. Fires the existing
    employee + admin notification email (non-blocking) on every download,
    mirroring app.py's /api/employee/salary-slip/download/<slip_id> route."""
    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT s.FILE_PATH, s.MONTH, s.YEAR, e.EMPLOYEE_NAME, e.EMAIL AS EMP_EMAIL
            FROM EMP_NRM_SALARY_SLIPS s
            LEFT JOIN EMP_NRM_EMPLOYEES e ON s.EMP_ID = e.EMPLOYEE_ID
            WHERE s.SLIP_ID = %s AND s.EMP_ID = %s
            LIMIT 1
            """,
            (slip_id, employee_id),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Slip not found")

        file_path = row.get("FILE_PATH") or ""
        if not file_path.startswith("s3://"):
            raise HTTPException(status_code=404, detail="Slip file not available")

        parts = file_path[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        s3c = _employee_s3_client()
        download_url = s3c.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300
        )

        emp_name = row.get("EMPLOYEE_NAME") or employee_id
        emp_email = row.get("EMP_EMAIL") or ""
        month = str(row.get("MONTH") or "")
        year = str(row.get("YEAR") or "")
        try:
            # _send_payslip_email spawns its own daemon thread internally,
            # so this call returns immediately — no need to wrap it again.
            _send_payslip_email(employee_id, emp_name, emp_email, month, year)
        except Exception as notify_err:
            print(f"⚠️ Payslip notify failed (non-fatal): {notify_err}")

        return {"success": True, "download_url": download_url}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Get payslip download error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            _emp_pool_return(conn)


# ══════════════════════════════════════════════════════════════
# RUN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)