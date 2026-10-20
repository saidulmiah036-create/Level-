# -*- coding: utf-8 -*-
"""
ROCKS Glass Command Center
Monochrome liquid-glass dashboard and real-time account status (aiohttp).
"""

import asyncio
import contextvars
import html
import json
import os
import re
import secrets
import hashlib
import hmac
import time
import shutil
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import aiohttp
from aiohttp import web

# Ban-check endpoint is intentionally isolated from the game login flow.
BAN_CHECK_URL = "https://api-check-ban-alliff-omega.vercel.app/api/check"
BAN_CHECK_INTERVAL_SECONDS = 60
BAN_CHECK_TIMEOUT_SECONDS = 8
BAN_CHECK_CONCURRENCY = 6
BAN_STATUS_CACHE: Dict[str, Dict[str, Any]] = {}
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))  # application/code directory (index.html lives here)
# Prefer explicitly configured storage, then Railway's mounted volume path.
# Without a mounted volume this falls back to PROJECT_DIR for local development.
DATA_DIR = os.path.abspath(
    os.environ.get("ROCKS_DATA_DIR", "").strip()
    or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    or PROJECT_DIR
)
os.makedirs(DATA_DIR, exist_ok=True)

def _persistent_storage_available() -> bool:
    """Best-effort check that the selected data path is a host-mounted volume."""
    railway_mount = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    if railway_mount:
        return os.path.abspath(railway_mount) == os.path.abspath(DATA_DIR)
    explicit = os.environ.get("ROCKS_DATA_DIR", "").strip()
    if explicit and os.path.abspath(DATA_DIR) != os.path.abspath(PROJECT_DIR):
        return os.path.ismount(DATA_DIR)
    return False

# If the host provides a top-level /accounts.json, use it directly; otherwise
# use the configured persistent data directory (Railway volume /data, etc.).
ROOT_ACCOUNTS_FILE = os.path.abspath(os.sep + "accounts.json")
ACCOUNT_FILE_ADMIN = (
    ROOT_ACCOUNTS_FILE if os.path.isfile(ROOT_ACCOUNTS_FILE)
    else os.path.join(DATA_DIR, "accounts.json")
)
_STORAGE_MIGRATION_MARKER = os.path.join(DATA_DIR, ".rocks_storage_migrated_v1")
_STORAGE_MIGRATION_DONE = False
_ACCOUNTS_MIGRATED = False

# Context-local ownership flows with each account worker and its child tasks.
LOG_OWNER_CONTEXT = contextvars.ContextVar("rocks_log_owner", default="")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_is_banned(payload: Any) -> Optional[bool]:
    """Accept only an explicit is_banned value; never guess on malformed replies."""
    if not isinstance(payload, dict) or "is_banned" not in payload:
        return None
    value = payload.get("is_banned")
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "banned"}:
            return True
        if normalized in {"false", "0", "no", "not banned", "not_banned", "active"}:
            return False
    return None


def _ban_uid_for_display(display_uid: str, owner_username: Optional[str] = None) -> str:
    """Prefer this tenant's saved guest UID when mapping API status to a dashboard card."""
    credential = bot_state.get_credential(str(display_uid), owner_username) if "bot_state" in globals() else None
    if isinstance(credential, dict) and credential.get("auth_uid"):
        return str(credential["auth_uid"]).strip()
    return str(display_uid).strip()


async def _collect_ban_uids() -> List[str]:
    targets = set()
    # Include every guest UID on disk, even if its login has not succeeded yet.
    for item in await _load_accounts_file():
        if isinstance(item, dict) and item.get("uid"):
            targets.add(str(item["uid"]).strip())
    # Include loaded display IDs and original auth UIDs without crossing tenant credentials.
    for account in list(bot_state.accounts.values()):
        if not isinstance(account, dict):
            continue
        display_uid = str(account.get("uid", "") or "").strip()
        owner = str(account.get("owner_username", "") or ADMIN_USERNAME)
        uid = _ban_uid_for_display(display_uid, owner)
        if uid:
            targets.add(uid)
        if display_uid:
            targets.add(display_uid)
    seen_credentials = set()
    for credential in list(bot_state.account_credentials.values()):
        if isinstance(credential, dict) and id(credential) not in seen_credentials:
            seen_credentials.add(id(credential))
            auth_uid = str(credential.get("auth_uid", "") or "").strip()
            account_id = str(credential.get("account_id", "") or "").strip()
            if auth_uid:
                targets.add(auth_uid)
            elif account_id:
                targets.add(account_id)
    targets.discard("")
    return sorted(targets)


async def _check_one_uid(session: aiohttp.ClientSession, uid: str, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        attempted_at = _utc_stamp()
        try:
            async with session.get(BAN_CHECK_URL, params={"uid": uid}) as response:
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"ban API returned HTTP {response.status}")
                payload = await response.json(content_type=None)
            is_banned = _read_is_banned(payload)
            if is_banned is None:
                raise ValueError("ban API response has no valid boolean is_banned field")
            previous = BAN_STATUS_CACHE.get(uid, {})
            result = {
                "uid": uid,
                "is_banned": is_banned,
                "ban_status": "BANNED" if is_banned else "NOT BANNED",
                "ban_checked_at": attempted_at,
                "ban_check_error": None,
            }
            BAN_STATUS_CACHE[uid] = result
            if isinstance(previous.get("is_banned"), bool) and previous["is_banned"] != is_banned:
                bot_state.log(f"Ban status changed for UID {uid}: {result['ban_status']}", "warning" if is_banned else "success", uid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Preserve the last confirmed status but record that the latest poll failed.
            previous = BAN_STATUS_CACHE.get(uid, {"uid": uid, "is_banned": None, "ban_status": "UNKNOWN", "ban_checked_at": None})
            previous["ban_check_error"] = f"{type(exc).__name__}: {exc}"
            previous["ban_last_attempt_at"] = attempted_at
            BAN_STATUS_CACHE[uid] = previous


async def _ban_checker_loop() -> None:
    timeout = aiohttp.ClientTimeout(total=BAN_CHECK_TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=BAN_CHECK_CONCURRENCY)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            cycle_started = time.monotonic()
            try:
                targets = await _collect_ban_uids()
                if targets:
                    semaphore = asyncio.Semaphore(BAN_CHECK_CONCURRENCY)
                    await asyncio.gather(*(_check_one_uid(session, uid, semaphore) for uid in targets))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                bot_state.log(f"Ban checker cycle failed: {type(exc).__name__}: {exc}", "warning")
            # Schedule at 60-second start-to-start cadence; a large/slow list may overrun.
            elapsed = time.monotonic() - cycle_started
            await asyncio.sleep(max(0.0, BAN_CHECK_INTERVAL_SECONDS - elapsed))


async def _ban_checker_context(app: web.Application):
    task = asyncio.create_task(_ban_checker_loop(), name="rocks-ban-checker")
    app["ban_checker_task"] = task
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# Global bot state shared between Main.py and Web Dashboard.
# Runtime entries are tenant-scoped: identical game display UIDs must never
# overwrite another dashboard user's status, credentials, or worker.
class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 200
        self.total_matches = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}

    def _owner(self, owner_username: Optional[str] = None) -> str:
        return str(owner_username or LOG_OWNER_CONTEXT.get() or globals().get("ADMIN_USERNAME", "admin")).strip() or "admin"

    def scoped_key(self, identifier: Any, owner_username: Optional[str] = None) -> str:
        owner = self._owner(owner_username)
        owner_key = _username_key(owner) if "_username_key" in globals() else owner.casefold()
        return f"{owner_key}::{str(identifier or '').strip()}"

    def worker_key(self, identifier: Any, owner_username: Optional[str] = None) -> str:
        return self.scoped_key(identifier, owner_username)

    def account_key(self, uid: Any, owner_username: Optional[str] = None) -> Optional[str]:
        raw = str(uid or "").strip()
        if not raw:
            return None
        # Allow internal scoped keys for route helpers that already resolved a record.
        if raw in self.accounts:
            return raw
        context_owner = owner_username or LOG_OWNER_CONTEXT.get()
        if context_owner:
            key = self.scoped_key(raw, context_owner)
            return key if key in self.accounts else None
        matches = [key for key, acc in self.accounts.items() if str(acc.get("uid", "")) == raw]
        return matches[0] if len(matches) == 1 else None

    def get_account(self, uid: Any, owner_username: Optional[str] = None) -> Optional[Dict[str, Any]]:
        key = self.account_key(uid, owner_username)
        return self.accounts.get(key) if key else None

    def get_credential(self, identifier: Any, owner_username: Optional[str] = None) -> Optional[Dict[str, Any]]:
        raw = str(identifier or "").strip()
        if not raw:
            return None
        context_owner = owner_username or LOG_OWNER_CONTEXT.get()
        if context_owner:
            return self.account_credentials.get(self.scoped_key(raw, context_owner))
        matches = []
        for key, value in self.account_credentials.items():
            if key.endswith("::" + raw) and isinstance(value, dict) and all(value is not old for old in matches):
                matches.append(value)
        return matches[0] if len(matches) == 1 else None

    def set_credential(self, identifier: Any, credential: Dict[str, Any], owner_username: Optional[str] = None) -> None:
        raw = str(identifier or "").strip()
        if not raw:
            return
        owner = self._owner(owner_username or credential.get("owner_username"))
        credential["owner_username"] = owner
        self.account_credentials[self.scoped_key(raw, owner)] = credential

    def remove_record_runtime(self, record: Dict[str, Any]) -> None:
        owner = str(record.get("owner_username", "") or globals().get("ADMIN_USERNAME", "admin"))
        identity_values = {
            str(record.get("uid", "") or "").strip(),
            str(record.get("token", "") or "").strip(),
            str(record.get("token", "") or "").strip()[:10],
        }
        identity_values.discard("")
        # Include this tenant's resolved display account ID, not aliases from other tenants.
        for credential in list(self.account_credentials.values()):
            if not isinstance(credential, dict) or _username_key(credential.get("owner_username", "")) != _username_key(owner):
                continue
            if (str(credential.get("auth_uid", "") or "").strip() in identity_values or
                    str(credential.get("auth_token", "") or "").strip() in identity_values):
                identity_values.add(str(credential.get("account_id", "") or "").strip())
                identity_values.add(f"tok_{str(credential.get('auth_token', '') or '')[:20]}".strip())
        for key, account in list(self.accounts.items()):
            if _username_key(account.get("owner_username", "")) == _username_key(owner) and (
                str(account.get("uid", "") or "").strip() in identity_values
            ):
                self.accounts.pop(key, None)
        for key, credential in list(self.account_credentials.items()):
            if isinstance(credential, dict) and _username_key(credential.get("owner_username", "")) == _username_key(owner):
                if (str(credential.get("auth_uid", "") or "").strip() in identity_values or
                        str(credential.get("auth_token", "") or "").strip() in identity_values or
                        str(credential.get("account_id", "") or "").strip() in identity_values):
                    self.account_credentials.pop(key, None)
        self.recalc_totals()

    def log(self, message: str, level: str = "info", uid: Optional[str] = None, owner_username: Optional[str] = None):
        owner = str(owner_username or LOG_OWNER_CONTEXT.get() or "").strip()
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "uid": str(uid) if uid is not None else None,
            "owner_username": owner or None,
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

    def register_account(self, uid: str, nickname: str, region: str, level: int, exp: int, likes: int = 0):
        uid_str = str(uid)
        owner = self._owner()
        key = self.scoped_key(uid_str, owner)
        lvl_val = max(1, int(level or 1))
        default_mode = "BR"
        if key not in self.accounts:
            self.accounts[key] = {
                "uid": uid_str,
                "owner_username": owner,
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "IND",
                "level": lvl_val,
                "initial_exp": exp,
                "current_exp": exp,
                "gained_exp": 0,
                "likes": likes or 0,
                "mode": default_mode,
                "mode_manual": False,
                "status": "ONLINE_IDLE",
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "last_updated": time.strftime("%H:%M:%S")
            }
        else:
            acc = self.accounts[key]
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level:
                acc["level"] = lvl_val
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["likes"] = likes
            acc["owner_username"] = owner
            if not acc.get("mode_manual", False):
                acc["mode"] = "BR"
            acc["status"] = "ONLINE_IDLE"
            acc["last_updated"] = time.strftime("%H:%M:%S")
        self.recalc_totals()

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        key = self.account_key(uid)
        if not key:
            return
        acc = self.accounts[key]
        uid_str = str(uid)
        old_exp = acc["current_exp"]
        acc["current_exp"] = current_exp
        if level is not None and level > 0:
            acc["level"] = int(level)
        if not acc.get("mode_manual", False):
            acc["mode"] = "BR"
        acc["gained_exp"] = max(0, current_exp - acc["initial_exp"])
        acc["last_updated"] = time.strftime("%H:%M:%S")
        diff = current_exp - old_exp
        if diff > 0:
            self.log(
                f"Account {acc['nickname']} ({uid_str}) gained +{diff} EXP! "
                f"Total Gained: +{acc['gained_exp']} | Mode: {acc['mode']}",
                "success", uid_str
            )
        self.recalc_totals()

    def get_account_level(self, uid: str, owner_username: Optional[str] = None) -> int:
        acc = self.get_account(uid, owner_username)
        return int(acc.get("level", 1) or 1) if acc else 1

    def get_account_mode(self, uid: str, owner_username: Optional[str] = None) -> str:
        acc = self.get_account(uid, owner_username)
        if not acc:
            return "BR"
        level = int(acc.get("level", 1) or 1)
        if level > 10:
            return "LONE_WOLF"
        mode = str(acc.get("mode", "BR") or "BR").upper()
        return "LONE_WOLF" if mode == "LONE_WOLF" and acc.get("mode_manual", False) else "BR"

    def set_account_mode(self, uid: str, mode: str, owner_username: Optional[str] = None):
        uid_str = str(uid)
        mode = str(mode or "").upper().replace("-", "_").replace(" ", "_")
        if mode in ("LW", "LONEWOLF", "LONE_WOLF"):
            mode = "LONE_WOLF"
        elif mode == "BR":
            mode = "BR"
        else:
            return False, "Invalid mode"
        acc = self.get_account(uid_str, owner_username)
        if not acc:
            return False, "Account not found"
        level = int(acc.get("level", 1) or 1)
        if level < 5 and mode != "BR":
            return False, "Lone Wolf is locked until Level 5"
        acc["mode"] = mode
        acc["mode_manual"] = True
        acc["last_updated"] = time.strftime("%H:%M:%S")
        self.log(f"UID {uid_str} ({acc.get('nickname', 'Unknown')}) switched to {mode}", "success", uid_str, owner_username=owner_username)
        return True, mode

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        key = self.account_key(uid)
        if not key:
            # Login workers often report state using the original guest UID, while
            # dashboard cards use the resolved in-game account ID. Resolve the alias
            # only within the active tenant context.
            credential = self.get_credential(uid)
            if credential:
                key = self.account_key(credential.get("account_id"), credential.get("owner_username"))
        if key:
            self.accounts[key]["status"] = status
            if active_matches is not None:
                self.accounts[key]["active_matches"] = active_matches
            self.accounts[key]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, uid: str):
        self.total_matches += 1
        key = self.account_key(uid)
        if key:
            acc = self.accounts[key]
            acc["matches_played"] += 1
            acc["last_match_time"] = time.strftime("%H:%M:%S")
            acc["last_updated"] = time.strftime("%H:%M:%S")
            self.log(f"Account {acc['nickname']} finished Match #{acc['matches_played']}", "info", str(uid))

    def recalc_totals(self):
        self.total_gained_exp = sum(acc.get("gained_exp", 0) for acc in self.accounts.values())


bot_state = BotState()

# Convenience cursor for the dashboard batch importer.
DEFAULT_BATCH_IMPORT_STATE = {"last_start": 0, "last_count": 0, "last_end": 0, "total": 0}
_BATCH_IMPORT_STATE_BY_USER: Dict[str, Dict[str, int]] = {}


def _migrate_legacy_storage_files() -> None:
    """Copy existing runtime JSON files into a mounted data directory once.

    This matters when a deployment switches from its ephemeral project directory
    to a persistent volume. Existing destination files always win; the migration
    never overwrites a file already present on the volume.
    """
    global _STORAGE_MIGRATION_DONE
    if _STORAGE_MIGRATION_DONE:
        return
    if os.path.abspath(DATA_DIR) == os.path.abspath(PROJECT_DIR):
        _STORAGE_MIGRATION_DONE = True
        return
    if os.path.isfile(_STORAGE_MIGRATION_MARKER):
        # A deployment may have created the migration marker before accounts.json
        # was allowed into the image build context. If the admin store is still
        # genuinely absent, import the project-root seed once without overwriting
        # any existing volume data (including an intentionally empty [] file).
        seed_source = os.path.join(PROJECT_DIR, "accounts.json")
        seed_destination = os.path.join(DATA_DIR, "accounts.json")
        if os.path.isfile(seed_source) and not os.path.exists(seed_destination):
            try:
                os.makedirs(DATA_DIR, exist_ok=True)
                shutil.copy2(seed_source, seed_destination)
                try:
                    os.chmod(seed_destination, 0o600)
                except OSError:
                    pass
                print("[STORAGE] Imported missing admin accounts.json seed into configured data directory.")
            except OSError as exc:
                raise RuntimeError(f"Could not import admin accounts.json seed into {DATA_DIR}: {type(exc).__name__}: {exc}") from exc
        _STORAGE_MIGRATION_DONE = True
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    candidates = {
        "accounts.json", "token_cache.json", "devices.json",
        "version_config_cache.json", "playstore_version_cache.json",
    }
    try:
        candidates.update(
            name for name in os.listdir(PROJECT_DIR)
            if name.startswith("accounts_") and name.endswith(".json")
        )
        for name in sorted(candidates):
            source = os.path.join(PROJECT_DIR, name)
            if not os.path.isfile(source):
                continue
            destination = os.path.join(DATA_DIR, name)
            if os.path.abspath(source) == os.path.abspath(destination) or os.path.exists(destination):
                continue
            shutil.copy2(source, destination)
            try:
                os.chmod(destination, 0o600)
            except OSError:
                pass
            print(f"[STORAGE] Migrated existing {name} into configured data directory.")

        with open(_STORAGE_MIGRATION_MARKER, "w", encoding="utf-8") as handle:
            json.dump({"version": 1, "completed_at": _utc_stamp()}, handle)
        try:
            os.chmod(_STORAGE_MIGRATION_MARKER, 0o600)
        except OSError:
            pass
        _STORAGE_MIGRATION_DONE = True
    except OSError as exc:
        # Fail loudly rather than serving empty portals while old data cannot be copied.
        raise RuntimeError(f"Could not migrate ROCKS data into {DATA_DIR}: {type(exc).__name__}: {exc}") from exc


# ==================== ANONYMOUS PER-BROWSER PROFILES ====================
# Dashboard username/password login has been removed. A random, HttpOnly browser
# cookie identifies a private anonymous profile so each browser gets its own data.
# This is convenience-level isolation, not strong identity verification: anyone
# with a copied cookie can use that profile. Keep the service private if needed.
ADMIN_USERNAME = "legacy_admin"
ANON_COOKIE = "rocks_profile"
ANON_SESSIONS: Dict[str, Dict[str, Any]] = {}
_PROFILE_RE = re.compile(r"^[A-Za-z0-9_.-]{3,40}$")


def _username_key(username: str) -> str:
    return str(username or "").strip().casefold()


def _ensure_profile_storage() -> None:
    """Anonymous profiles are created by the request middleware; no user registry is stored."""
    return None


def _anonymous_session(token: str) -> Dict[str, Any]:
    session = ANON_SESSIONS.get(token)
    if session:
        return session
    profile = "guest_" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]
    session = {
        "username_key": profile,
        "csrf": secrets.token_urlsafe(24),
        "created_at": time.time(),
        "expires_at": float("inf"),
        "user": {"username": profile, "name": "Guest", "role": "user", "created_at": _utc_stamp()},
    }
    ANON_SESSIONS[token] = session
    return session


@web.middleware
async def auth_middleware(request: web.Request, handler):
    # No username/password screen. Each browser gets an opaque random profile token.
    token = request.cookies.get(ANON_COOKIE, "")
    is_new = not re.fullmatch(r"[A-Za-z0-9_-]{40,100}", token or "")
    if is_new:
        token = secrets.token_urlsafe(32)
    session = _anonymous_session(token)
    request["auth_session"] = session
    request["current_user"] = session["user"]

    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.path != "/health":
        expected = str(session.get("csrf", ""))
        supplied = request.headers.get("X-CSRF-Token", "")
        if not expected or not hmac.compare_digest(expected, supplied):
            return web.json_response({"status": "error", "error": "Invalid or missing request token. Refresh the dashboard and try again."}, status=403)

    response = await handler(request)
    if is_new:
        secure = request.secure or request.headers.get("X-Forwarded-Proto", "").split(",", 1)[0].strip().lower() == "https"
        response.set_cookie(ANON_COOKIE, token, max_age=31536000, httponly=True, secure=secure, samesite="Strict", path="/")
    return response


# ==================== HTTP HANDLERS ====================

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")

async def handle_index(request: web.Request) -> web.Response:
    user = request.get("current_user") or {"role": "user", "username": "guest"}
    if os.path.exists(TEMPLATE_PATH):
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            content = f.read()
    else:
        content = "<h1>index.html not found in the project root.</h1>"
    session = request.get("auth_session") or {}
    csrf = html.escape(str(session.get("csrf", "")), quote=True)
    content = content.replace(
        "</head>",
        f'<meta name="rocks-role" content="user"><meta name="rocks-csrf" content="{csrf}"></head>',
        1,
    )
    # Strip legacy admin/login chrome even if an old cached index.html is deployed.
    content = re.sub(r'\s*<button class="nav-item" data-page="admin"[^>]*>.*?</button>', "", content, count=1, flags=re.S)
    content = re.sub(r'\s*<section id="page-admin"[^>]*>.*?</section>', "", content, count=1, flags=re.S)
    content = re.sub(r'<div class="auth-tools".*?</div>', "", content, count=1, flags=re.S)
    return web.Response(text=content, content_type="text/html", charset="utf-8", headers={"Cache-Control": "no-store"})

def _current_username_key(request: web.Request) -> str:
    user = request.get("current_user") or {}
    username = str(user.get("username", "") or "").strip()
    return _username_key(username) or _username_key(ADMIN_USERNAME)


def _record_owner_key(record: Dict[str, Any]) -> str:
    # Older account entries predate multi-user ownership and belong to admin.
    owner = str(record.get("owner_username", "") or "").strip()
    return _username_key(owner) if owner else _username_key(ADMIN_USERNAME)


def _user_owns_record(user: Dict[str, Any], record: Dict[str, Any]) -> bool:
    return user.get("role") == "admin" or _record_owner_key(record) == _username_key(user.get("username", ""))


def _record_matches_identity(record: Dict[str, Any], identities: set[str]) -> bool:
    uid = str(record.get("uid", "") or "").strip()
    token = str(record.get("token", "") or "").strip()
    return bool((uid and uid in identities) or (token and token in identities))


def _visible_records(user: Dict[str, Any], records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    valid_records = [record for record in records if isinstance(record, dict)]
    if user.get("role") == "admin":
        return valid_records
    return [record for record in valid_records if _user_owns_record(user, record)]


def _identity_values_for_records(records: List[Dict[str, Any]]) -> set[str]:
    identities: set[str] = set()
    for row in records:
        if not isinstance(row, dict):
            continue
        uid = str(row.get("uid", "") or "").strip()
        token = str(row.get("token", "") or "").strip()
        owner = str(row.get("owner_username", "") or ADMIN_USERNAME)
        if uid:
            identities.add(uid)
        if token:
            identities.update({token, token[:10], f"TOKEN-{token[:10]}"})
        for identifier in (uid, token, f"tok_{token[:20]}" if token else ""):
            credential = bot_state.get_credential(identifier, owner) if identifier else None
            if isinstance(credential, dict):
                account_id = str(credential.get("account_id", "") or "").strip()
                auth_uid = str(credential.get("auth_uid", "") or "").strip()
                auth_token = str(credential.get("auth_token", "") or "").strip()
                identities.update(value for value in (account_id, auth_uid, auth_token) if value)
    return identities


def _record_for_display_uid(display_uid: str, records: List[Dict[str, Any]], owner_username: Optional[str] = None) -> Optional[Dict[str, Any]]:
    target = str(display_uid or "").strip()
    if not target:
        return None
    owner_key = _username_key(owner_username) if owner_username else None
    for record in records:
        if not isinstance(record, dict):
            continue
        record_owner = str(record.get("owner_username", "") or ADMIN_USERNAME)
        if owner_key is not None and _username_key(record_owner) != owner_key:
            continue
        record_uid = str(record.get("uid", "") or "").strip()
        record_token = str(record.get("token", "") or "").strip()
        token_display_id = f"TOKEN-{record_token[:10]}" if record_token else ""
        if target == record_uid or target == record_token or (token_display_id and target.casefold() == token_display_id.casefold()):
            return record
        # Alias resolution must use credentials owned by the same tenant as this row.
        for identifier in (target, record_uid, record_token, f"tok_{record_token[:20]}" if record_token else ""):
            credential = bot_state.get_credential(identifier, record_owner) if identifier else None
            if not isinstance(credential, dict):
                continue
            auth_uid = str(credential.get("auth_uid", "") or "").strip()
            auth_token = str(credential.get("auth_token", "") or "").strip()
            account_id = str(credential.get("account_id", "") or "").strip()
            if (target == account_id and ((auth_uid and auth_uid == record_uid) or (auth_token and auth_token == record_token))) or (auth_uid and auth_uid == record_uid) or (auth_token and auth_token == record_token):
                return record
    return None


def _user_can_access_uid(user: Dict[str, Any], uid: str, records: List[Dict[str, Any]]) -> bool:
    if user.get("role") == "admin":
        return True
    visible = _visible_records(user, records)
    return _record_for_display_uid(uid, visible, str(user.get("username", ""))) is not None


def _visible_log_entries(user: Dict[str, Any], records: List[Dict[str, Any]], display_ids: set[str]) -> List[Dict[str, Any]]:
    if user.get("role") == "admin":
        return bot_state.logs[-60:]
    user_key = _username_key(user.get("username", ""))
    identity_values = _identity_values_for_records(records) | display_ids
    visible = []
    for entry in bot_state.logs:
        owner_key = _username_key(entry.get("owner_username", ""))
        uid = str(entry.get("uid", "") or "").strip()
        if owner_key == user_key or (not owner_key and uid and uid in identity_values):
            visible.append(entry)
    return visible[-60:]


async def handle_get_stats(request: web.Request) -> web.Response:
    user = request.get("current_user") or {}
    all_records = await _load_accounts_file()
    user_records = _visible_records(user, all_records)
    identities = _identity_values_for_records(user_records)
    is_admin = user.get("role") == "admin"

    accounts_data = []
    display_ids: set[str] = set()
    for original in bot_state.accounts.values():
        if not isinstance(original, dict):
            continue
        account = dict(original)
        display_uid = str(account.get("uid", "") or "").strip()
        account_owner = str(account.get("owner_username", "") or ADMIN_USERNAME)
        # Runtime state is tenant-scoped. Never infer a user's status from another
        # tenant's credential aliases or from a coincidentally equal display UID.
        if not is_admin and _username_key(account_owner) != _username_key(user.get("username", "")):
            continue
        query_uid = _ban_uid_for_display(display_uid, account_owner)
        record_owned = _record_for_display_uid(display_uid, user_records, owner_username=account_owner)
        if record_owned is None:
            # The saved row may use the auth UID while the runtime card uses a game ID;
            # resolve again against this tenant's auth identifier only.
            record_owned = next((row for row in user_records if (
                str(row.get("uid", "") or "").strip() == query_uid or
                (str(row.get("token", "") or "").strip() and query_uid == f"TOKEN-{str(row.get('token', '')).strip()[:10]}")
            )), None)
        if record_owned is None:
            continue
        ban_info = BAN_STATUS_CACHE.get(query_uid) or BAN_STATUS_CACHE.get(display_uid) or {}
        account["auth_uid"] = query_uid
        account["is_paused"] = bool(record_owned.get("is_paused"))
        if account["is_paused"]:
            account["status"] = "PAUSED"
        account["is_banned"] = ban_info.get("is_banned") if isinstance(ban_info.get("is_banned"), bool) else None
        account["ban_status"] = ban_info.get("ban_status", "UNKNOWN")
        account["ban_checked_at"] = ban_info.get("ban_checked_at")
        account["ban_check_error"] = ban_info.get("ban_check_error")
        accounts_data.append(account)
        if display_uid:
            display_ids.add(display_uid)
        if query_uid:
            display_ids.add(query_uid)

    # Include only the signed-in user's configured accounts awaiting login.
    known_uids = set()
    for item in accounts_data:
        for key in ("uid", "auth_uid"):
            value = str(item.get(key, "") or "").strip()
            if value:
                known_uids.add(value)
    for configured in user_records:
        if not isinstance(configured, dict):
            continue
        guest_uid = str(configured.get("uid", "") or "").strip()
        saved_token = str(configured.get("token", "") or "").strip()
        display_id = guest_uid or (f"TOKEN-{saved_token[:10]}" if saved_token else "")
        if not display_id or display_id in known_uids:
            continue
        # Ban API accepts UIDs, not access tokens. Token-only rows stay UNKNOWN until login resolves their account ID.
        ban_info = BAN_STATUS_CACHE.get(guest_uid, {}) if guest_uid else {}
        accounts_data.append({
            "uid": display_id, "auth_uid": guest_uid or display_id,
            "nickname": "Login pending", "region": str(configured.get("region", "IND") or "IND"), "level": 1,
            "initial_exp": 0, "current_exp": 0, "gained_exp": 0,
            "likes": 0, "mode": str(configured.get("mode", "BR") or "BR"), "mode_manual": bool(configured.get("mode_manual", False)),
            "status": "PAUSED" if configured.get("is_paused") else "LOGIN_PENDING", "is_paused": bool(configured.get("is_paused")), "matches_played": 0,
            "active_matches": 0, "last_match_time": None,
            "last_updated": None, "is_configured_only": True, "is_token_only": bool(saved_token and not guest_uid),
            "is_banned": ban_info.get("is_banned") if isinstance(ban_info.get("is_banned"), bool) else None,
            "ban_status": ban_info.get("ban_status", "UNKNOWN"),
            "ban_checked_at": ban_info.get("ban_checked_at"),
            "ban_check_error": ban_info.get("ban_check_error"),
        })
        known_uids.add(display_id)
        display_ids.add(display_id)

    accounts_data.sort(key=lambda x: (bool(x.get("is_configured_only")), -int(x.get("gained_exp", 0) or 0)))
    own_matches = sum(int(acc.get("matches_played", 0) or 0) for acc in accounts_data)
    own_exp = sum(int(acc.get("gained_exp", 0) or 0) for acc in accounts_data)
    own_active = sum(int(acc.get("active_matches", 0) or 0) for acc in accounts_data)
    uptime = max(1, int(time.time() - bot_state.start_time))
    logs = _visible_log_entries(user, user_records, display_ids)
    import_state = _BATCH_IMPORT_STATE_BY_USER.get(_current_username_key(request), dict(DEFAULT_BATCH_IMPORT_STATE))
    return web.json_response({
        "total_accounts": len(accounts_data),
        "total_matches": own_matches,
        "total_matches_started": own_matches,
        "total_gained_exp": own_exp,
        "total_active_matches": own_active,
        "exp_per_hour": int(own_exp / (uptime / 3600)),
        "accounts": accounts_data,
        "logs": logs,
        "uptime": uptime,
        "import_state": dict(import_state),
    })


async def handle_add_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({"status": "error", "error": "Invalid payload"}, status=400)
        user = request.get("current_user") or {}
        owner_username = str(user.get("username", ADMIN_USERNAME))
        all_records = await _load_accounts_file()
        own_records = await _load_user_accounts_file(user)

        if "uid" in data and "password" in data:
            uid = str(data["uid"]).strip()
            pwd = str(data["password"]).strip()
            if not uid or not pwd:
                return web.json_response({"status": "error", "error": "UID and Password are required"}, status=400)
            matched = [acc for acc in all_records if str(acc.get("uid", "") or "").strip() == uid]
            if any(_record_owner_key(acc) != _username_key(owner_username) for acc in matched):
                return web.json_response({"status": "error", "error": "That account is assigned to another user."}, status=409)
            own_records = [acc for acc in own_records if str(acc.get("uid", "") or "").strip() != uid]
            payload = {"uid": uid, "password": pwd, "owner_username": owner_username}
        elif "token" in data:
            token = str(data["token"]).strip()
            if not token:
                return web.json_response({"status": "error", "error": "Token is required"}, status=400)
            matched = [acc for acc in all_records if str(acc.get("token", "") or "").strip() == token]
            if any(_record_owner_key(acc) != _username_key(owner_username) for acc in matched):
                return web.json_response({"status": "error", "error": "That account is assigned to another user."}, status=409)
            own_records = [acc for acc in own_records if str(acc.get("token", "") or "").strip() != token]
            payload = {"token": token, "owner_username": owner_username}
        else:
            return web.json_response({"status": "error", "error": "Invalid payload"}, status=400)

        own_records.append(payload)
        _save_user_accounts_file(user, own_records)
        if "on_account_added" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_added"](dict(payload)))
        bot_state.log(f"New account added: {payload.get('uid') or 'Token account'}", "success", payload.get("uid"), owner_username=owner_username)
        return web.json_response({"status": "ok", "storage": os.path.basename(_account_filename_for_username(owner_username))})
    except Exception as e:
        return web.json_response({"status": "error", "error": f"Account add failed: {type(e).__name__}"}, status=500)


async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({"status": "error", "error": "Invalid payload"}, status=400)
        user = request.get("current_user") or {}
        uid = str(data.get("uid", "") or "").strip()
        auth_uid = str(data.get("auth_uid", "") or "").strip()
        token = str(data.get("token", "") or "").strip()
        if not uid and not auth_uid and not token:
            return web.json_response({"status": "error", "error": "Account identifier required"}, status=400)
        existing = await _load_accounts_file()
        candidates = [row for row in existing if (
            (uid and str(row.get("uid", "") or "").strip() in {uid, auth_uid}) or
            (auth_uid and str(row.get("uid", "") or "").strip() == auth_uid) or
            (token and str(row.get("token", "") or "").strip() == token) or
            (uid and str(row.get("token", "") or "").strip() and uid.casefold() == f"TOKEN-{str(row.get('token', '')).strip()[:10]}".casefold())
        )]
        owned_candidates = [row for row in candidates if _user_owns_record(user, row)]
        if not owned_candidates:
            return web.json_response({"status": "error", "error": "Account not found in your portal"}, status=404)
        to_remove = set()
        for row in owned_candidates:
            row_uid = str(row.get("uid", "") or "").strip()
            row_token = str(row.get("token", "") or "").strip()
            to_remove.add(("uid", row_uid, _record_owner_key(row)) if row_uid else ("token", row_token, _record_owner_key(row)))
        kept = []
        for row in existing:
            row_uid = str(row.get("uid", "") or "").strip()
            row_token = str(row.get("token", "") or "").strip()
            marker = ("uid", row_uid, _record_owner_key(row)) if row_uid else ("token", row_token, _record_owner_key(row))
            if marker not in to_remove:
                kept.append(row)
        # Stop every worker/alias before removing the saved row, including token workers.
        await _stop_record_workers(owned_candidates)
        _save_all_account_files(kept)
        for row in owned_candidates:
            bot_state.remove_record_runtime(row)
        bot_state.log(f"Account {auth_uid or uid or 'token account'} removed from rotation.", "warning", auth_uid or uid or None, owner_username=user.get("username", ADMIN_USERNAME))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": f"Account delete failed: {type(e).__name__}"}, status=500)


def _account_filename_for_username(username: str) -> str:
    username = str(username or "").strip()
    if not _PROFILE_RE.fullmatch(username):
        raise ValueError("Invalid profile identifier for account storage")
    if _username_key(username) == _username_key(ADMIN_USERNAME):
        return ACCOUNT_FILE_ADMIN
    return os.path.join(DATA_DIR, f"accounts_{username}.json")


def _read_account_rows(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return [dict(item) for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
    except FileNotFoundError:
        return []
    except (OSError, ValueError, TypeError):
        return []


def _write_account_rows(path: str, accounts: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or DATA_DIR, exist_ok=True)
    temp_path = path + f".{os.getpid()}.{secrets.token_hex(4)}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(accounts, handle, indent=2, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(temp_path, 0o600)
    except OSError:
        pass
    os.replace(temp_path, path)


def _account_identity(row: Dict[str, Any]) -> tuple[str, str]:
    uid = str(row.get("uid", "") or "").strip()
    token = str(row.get("token", "") or "").strip()
    return ("uid", uid) if uid else ("token", token)


def _ensure_account_storage_split() -> None:
    """Move any older owner-tagged rows out of accounts.json into per-user files once."""
    global _ACCOUNTS_MIGRATED
    _migrate_legacy_storage_files()
    if _ACCOUNTS_MIGRATED:
        return
    admin_key = _username_key(ADMIN_USERNAME)
    legacy = _read_account_rows(ACCOUNT_FILE_ADMIN)
    admin_rows: List[Dict[str, Any]] = []
    moved: Dict[str, List[Dict[str, Any]]] = {}
    for raw in legacy:
        row = dict(raw)
        owner = str(row.get("owner_username", "") or "").strip() or ADMIN_USERNAME
        row["owner_username"] = owner
        if _username_key(owner) == admin_key:
            admin_rows.append(row)
        else:
            path = _account_filename_for_username(owner)
            moved.setdefault(path, []).append(row)

    for path, incoming in moved.items():
        existing = _read_account_rows(path)
        existing_keys = {_account_identity(row) for row in existing}
        for row in incoming:
            marker = _account_identity(row)
            if marker not in existing_keys:
                existing.append(row)
                existing_keys.add(marker)
        _write_account_rows(path, existing)
    if legacy != admin_rows or not os.path.exists(ACCOUNT_FILE_ADMIN):
        _write_account_rows(ACCOUNT_FILE_ADMIN, admin_rows)

    # Ensure each provisioned user's tenant file exists, initially empty.
        _ACCOUNTS_MIGRATED = True


async def _load_user_accounts_file(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    _ensure_profile_storage()
    _ensure_account_storage_split()
    username = str(user.get("username", ADMIN_USERNAME))
    path = _account_filename_for_username(username)
    if not os.path.exists(path):
        _write_account_rows(path, [])
    rows = _read_account_rows(path)
    for row in rows:
        # The tenant file is authoritative. A row can never claim another tenant owner.
        row["owner_username"] = username
    return rows


def _save_user_accounts_file(user: Dict[str, Any], accounts: List[Dict[str, Any]]) -> None:
    _ensure_profile_storage()
    _ensure_account_storage_split()
    username = str(user.get("username", ADMIN_USERNAME))
    rows = []
    for source in accounts:
        if not isinstance(source, dict):
            continue
        row = dict(source)
        row["owner_username"] = username
        rows.append(row)
    _write_account_rows(_account_filename_for_username(username), rows)


async def _load_accounts_file() -> List[Dict[str, Any]]:
    """Administrator/internal aggregate. User-facing handlers must filter by owner."""
    _ensure_profile_storage()
    _ensure_account_storage_split()
    records: List[Dict[str, Any]] = []
    for path in [ACCOUNT_FILE_ADMIN] + sorted(
        os.path.join(DATA_DIR, name)
        for name in os.listdir(DATA_DIR)
        if name.startswith("accounts_") and name.endswith(".json") and not name.endswith(".tmp")
    ):
        if not os.path.isfile(path):
            continue
        rows = _read_account_rows(path)
        inferred_owner = ADMIN_USERNAME
        if path != ACCOUNT_FILE_ADMIN:
            inferred_owner = os.path.basename(path)[len("accounts_"):-len(".json")]
        for source in rows:
            row = dict(source)
            row["owner_username"] = inferred_owner if path != ACCOUNT_FILE_ADMIN else (str(row.get("owner_username", "") or "").strip() or ADMIN_USERNAME)
            records.append(row)
    return records


def _save_accounts_file(accounts: List[Dict[str, Any]]) -> None:
    """Legacy-compatible admin-only writer; tenant-aware handlers use _save_all_account_files."""
    admin_key = _username_key(ADMIN_USERNAME)
    rows = []
    for source in accounts:
        if not isinstance(source, dict):
            continue
        if _record_owner_key(source) == admin_key:
            row = dict(source)
            row["owner_username"] = ADMIN_USERNAME
            rows.append(row)
    _write_account_rows(ACCOUNT_FILE_ADMIN, rows)


def _save_all_account_files(accounts: List[Dict[str, Any]]) -> None:
    """Persist an aggregate list back to the separate file belonging to each owner."""
    _ensure_profile_storage()
    _ensure_account_storage_split()
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    owner_names: Dict[str, str] = {_username_key(ADMIN_USERNAME): ADMIN_USERNAME}
    for source in accounts:
        if not isinstance(source, dict):
            continue
        owner = str(source.get("owner_username", "") or "").strip() or ADMIN_USERNAME
        key = _username_key(owner)
        # Preserve old files owned by accounts for users who may have been removed from auth.
        owner_names.setdefault(key, owner)
        canonical = owner_names[key]
        row = dict(source)
        row["owner_username"] = canonical
        grouped.setdefault(key, []).append(row)
    # Keep known tenant files on disk, even when a portal has no remaining accounts.
    existing_files = [name for name in os.listdir(DATA_DIR) if name.startswith("accounts_") and name.endswith(".json")]
    for filename in existing_files:
        owner = filename[len("accounts_"):-len(".json")]
        owner_names.setdefault(_username_key(owner), owner)
    for key, owner in owner_names.items():
        path = _account_filename_for_username(owner)
        _write_account_rows(path, grouped.get(key, []))


def _normalize_uploaded_accounts(payload: Any) -> tuple[List[Dict[str, str]], Optional[str]]:
    if isinstance(payload, dict):
        for key in ("accounts", "data", "users"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
    if not isinstance(payload, list):
        return [], "JSON must contain an account list (array or accounts/data/users array)"

    result: List[Dict[str, str]] = []
    for pos, item in enumerate(payload, 1):
        if not isinstance(item, dict):
            return [], f"Account #{pos} is not an object"
        uid = str(item.get("uid", "") or "").strip()
        password = str(item.get("password", "") or item.get("pass", "") or "").strip()
        token = str(item.get("token", "") or "").strip()
        if uid and password:
            result.append({"uid": uid, "password": password})
        elif token:
            result.append({"token": token})
        else:
            return [], f"Account #{pos} needs uid + password, or token"
    return result, None


async def handle_batch_import(request: web.Request) -> web.Response:
    try:
        user = request.get("current_user") or {}
        owner_username = str(user.get("username", ADMIN_USERNAME))
        reader = await request.multipart()
        file_bytes = None
        file_filename = ""
        start_raw, count_raw = "1", "1"
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                file_filename = part.filename or ""
                file_bytes = await part.read(decode=False)
            elif part.name == "start":
                start_raw = (await part.text()).strip() or "1"
            elif part.name == "count":
                count_raw = (await part.text()).strip() or "1"

        if file_bytes is None or not file_filename:
            return web.json_response({"status": "error", "error": "Please select a JSON file"}, status=400)
        try:
            start, count = int(start_raw), int(count_raw)
        except ValueError:
            return web.json_response({"status": "error", "error": "Start and count must be whole numbers"}, status=400)
        if start < 1 or count < 1:
            return web.json_response({"status": "error", "error": "Start must be >= 1 and count must be >= 1"}, status=400)
        if count > 500:
            return web.json_response({"status": "error", "error": "Maximum 500 accounts per batch"}, status=400)
        if len(file_bytes) > 2 * 1024 * 1024:
            return web.json_response({"status": "error", "error": "JSON file is too large (max 2 MB)"}, status=400)
        try:
            payload = json.loads(file_bytes.decode("utf-8-sig"))
        except Exception as exc:
            return web.json_response({"status": "error", "error": f"Invalid JSON: {type(exc).__name__}"}, status=400)

        source, err = _normalize_uploaded_accounts(payload)
        if err:
            return web.json_response({"status": "error", "error": err}, status=400)
        total = len(source)
        if not total:
            return web.json_response({"status": "error", "error": "JSON contains no accounts"}, status=400)
        if start > total:
            return web.json_response({"status": "error", "error": f"Start account #{start} is outside the JSON (total {total})"}, status=400)

        end = min(total, start + count - 1)
        selected = source[start - 1:end]
        all_records = await _load_accounts_file()
        own_records = await _load_user_accounts_file(user)
        current_key = _username_key(owner_username)

        # Uniqueness is global, but records are written only to the current user's tenant file.
        for acc in selected:
            field = "uid" if "uid" in acc else "token"
            value = str(acc.get(field, "") or "").strip()
            conflicts = [row for row in all_records if str(row.get(field, "") or "").strip() == value]
            if any(_record_owner_key(row) != current_key for row in conflicts):
                return web.json_response({"status": "error", "error": f"An imported account is already assigned to another user ({field})."}, status=409)

        for acc in selected:
            row = dict(acc, owner_username=owner_username)
            field = "uid" if "uid" in row else "token"
            value = str(row.get(field, "") or "").strip()
            own_records = [item for item in own_records if not (str(item.get(field, "") or "").strip() == value)]
            own_records.append(row)
        _save_user_accounts_file(user, own_records)

        _BATCH_IMPORT_STATE_BY_USER[current_key] = {"last_start": start, "last_count": len(selected), "last_end": end, "total": total}
        callback = bot_state.refresh_callbacks.get("on_account_added")
        started = 0
        if callback:
            for acc in selected:
                owned_payload = dict(acc, owner_username=owner_username)
                await callback(owned_payload)
                started += 1
                if started < len(selected):
                    await asyncio.sleep(0.20)
        bot_state.log(f"Batch import: accounts {start}-{end} selected from {total}; {len(selected)} loaded.", "success", owner_username=owner_username)
        return web.json_response({"status": "ok", "total": total, "start": start, "count": len(selected), "end": end, "next_start": end + 1 if end < total else None, "started": started, "message": f"Accounts {start}-{end} imported"})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Batch import failed: {type(exc).__name__}"}, status=500)


async def handle_delete_all_accounts(request: web.Request) -> web.Response:
    try:
        user = request.get("current_user") or {}
        accounts = await _load_accounts_file()
        owned = _visible_records(user, accounts)
        removed = len(owned)
        kept = [row for row in accounts if row not in owned]
        _save_all_account_files(kept)
        await _stop_record_workers(owned)
        for row in owned:
            bot_state.remove_record_runtime(row)
        bot_state.log(f"Deleted {removed} account(s) from this portal.", "warning", owner_username=user.get("username", ADMIN_USERNAME))
        return web.json_response({"status": "ok", "removed": removed})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Delete-all failed: {type(exc).__name__}"}, status=500)


async def handle_clear_logs(request: web.Request) -> web.Response:
    user = request.get("current_user") or {}
    records = await _load_accounts_file()
    user_records = _visible_records(user, records)
    if user.get("role") == "admin":
        removed = len(bot_state.logs)
        bot_state.logs.clear()
    else:
        identities = _identity_values_for_records(user_records)
        user_key = _username_key(user.get("username", ""))
        kept = []
        removed = 0
        for entry in bot_state.logs:
            owner_key = _username_key(entry.get("owner_username", ""))
            uid = str(entry.get("uid", "") or "").strip()
            belongs = owner_key == user_key or (not owner_key and uid and uid in identities)
            if belongs:
                removed += 1
            else:
                kept.append(entry)
        bot_state.logs[:] = kept
    bot_state.log(f"Cleared {removed} log entry/entries from this portal.", "info", owner_username=user.get("username", ADMIN_USERNAME))
    return web.json_response({"status": "ok", "removed": removed})


async def handle_health(request: web.Request) -> web.Response:
    # Public health check intentionally omits account counts and tenant data.
    data_dir_configured = _persistent_storage_available()
    return web.json_response({
        "status": "ok",
        "uptime": int(time.time() - bot_state.start_time),
        "data_directory_configured": data_dir_configured,
    })


async def handle_refresh_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "") or "").strip()
        user = request.get("current_user") or {}
        records = await _load_accounts_file()
        if not uid or not _user_can_access_uid(user, uid, records):
            return web.json_response({"status": "error", "error": "Account not found in your portal"}, status=404)
        callback = bot_state.refresh_callbacks.get("on_refresh_account")
        if callback:
            owner_token = LOG_OWNER_CONTEXT.set(str(user.get("username", ADMIN_USERNAME)))
            try:
                asyncio.create_task(callback(uid))
            finally:
                LOG_OWNER_CONTEXT.reset(owner_token)
        return web.json_response({"status": "ok"})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Refresh failed: {type(exc).__name__}"}, status=400)


async def handle_mode_switch(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "") or "").strip()
        mode = str(data.get("mode", "") or "").strip()
        user = request.get("current_user") or {}
        records = await _load_accounts_file()
        if not uid or not _user_can_access_uid(user, uid, records):
            return web.json_response({"status": "error", "error": "Account not found in your portal"}, status=404)
        ok, result = bot_state.set_account_mode(uid, mode, owner_username=str(user.get("username", ADMIN_USERNAME)))
        if not ok:
            return web.json_response({"status": "error", "error": result}, status=400)
        target = _record_for_display_uid(uid, _visible_records(user, records))
        if target is not None:
            target["mode"] = result
            target["mode_manual"] = True
            _save_all_account_files(records)
        return web.json_response({"status": "ok", "uid": uid, "mode": result, "level": bot_state.get_account_level(uid, owner_username=str(user.get("username", ADMIN_USERNAME)))})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Mode update failed: {type(exc).__name__}"}, status=400)


def _worker_keys_for_record(record: Dict[str, Any]) -> set[str]:
    """Return only this tenant's worker/runtime keys for a saved account row."""
    row_uid = str(record.get("uid", "") or "").strip()
    row_token = str(record.get("token", "") or "").strip()
    owner = str(record.get("owner_username", "") or ADMIN_USERNAME)
    keys: set[str] = set()
    if row_uid:
        keys.add(bot_state.worker_key(row_uid, owner))
    if row_token:
        keys.add(bot_state.worker_key(row_token[:10], owner))

    seen = set()
    for credential in list(bot_state.account_credentials.values()):
        if not isinstance(credential, dict) or id(credential) in seen:
            continue
        seen.add(id(credential))
        if _username_key(credential.get("owner_username", "")) != _username_key(owner):
            continue
        auth_uid = str(credential.get("auth_uid", "") or "").strip()
        account_id = str(credential.get("account_id", "") or "").strip()
        auth_token = str(credential.get("auth_token", "") or "").strip()
        if (row_uid and auth_uid == row_uid) or (row_token and auth_token == row_token):
            if auth_uid:
                keys.add(bot_state.worker_key(auth_uid, owner))
            if account_id:
                keys.add(bot_state.worker_key(account_id, owner))
            if auth_token:
                keys.add(bot_state.worker_key(auth_token[:10], owner))
    return {key for key in keys if key}


async def _stop_record_workers(records: List[Dict[str, Any]]) -> int:
    keys: set[str] = set()
    for record in records:
        keys.update(_worker_keys_for_record(record))
    stopped: List[asyncio.Task] = []
    for key in keys:
        worker = bot_state.account_workers.get(key)
        if worker is None:
            continue
        bot_state.account_workers.pop(key, None)
        if not worker.done():
            worker.cancel()
            stopped.append(worker)
    if stopped:
        await asyncio.gather(*stopped, return_exceptions=True)
    # Preserve the dashboard card and its metrics while clearly marking it paused.
    for record in records:
        for key in _worker_keys_for_record(record):
            account = bot_state.accounts.get(key) or bot_state.get_account(key, owner_username=record.get("owner_username"))
            if account is not None:
                account["status"] = "PAUSED"
    return len(stopped)


async def _start_record_workers(records: List[Dict[str, Any]]) -> int:
    callback = bot_state.refresh_callbacks.get("on_account_added")
    if callback is None:
        return 0
    started = 0
    for record in records:
        payload = dict(record)
        payload.pop("is_paused", None)
        payload["owner_username"] = str(record.get("owner_username", ADMIN_USERNAME) or ADMIN_USERNAME)
        if not ((payload.get("uid") and payload.get("password")) or payload.get("token")):
            continue
        try:
            await callback(payload)
            started += 1
        except Exception as exc:
            bot_state.log(f"Could not start account worker: {type(exc).__name__}", "error", payload.get("uid"), owner_username=payload["owner_username"])
    return started


async def _find_owned_record(user: Dict[str, Any], uid: str, all_records: Optional[List[Dict[str, Any]]] = None) -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    records = all_records if all_records is not None else await _load_accounts_file()
    visible = _visible_records(user, records)
    target = _record_for_display_uid(uid, visible)
    if target is None:
        # Before login succeeds, a card displays the configured guest UID itself.
        target = next((row for row in visible if str(row.get("uid", "") or "").strip() == str(uid).strip()), None)
    return target, records


async def handle_pause_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "") or "").strip() if isinstance(data, dict) else ""
        user = request.get("current_user") or {}
        if not uid:
            return web.json_response({"status": "error", "error": "Account identifier required"}, status=400)
        row, all_records = await _find_owned_record(user, uid)
        if row is None:
            return web.json_response({"status": "error", "error": "Account not found in your portal"}, status=404)
        was_paused = bool(row.get("is_paused"))
        now_paused = not was_paused
        row["is_paused"] = now_paused
        _save_all_account_files(all_records)
        if now_paused:
            await _stop_record_workers([row])
        else:
            await _start_record_workers([row])
            for key in _worker_keys_for_record(row):
                account = bot_state.accounts.get(key) or bot_state.get_account(key, owner_username=row.get("owner_username"))
                if account is not None:
                    account["status"] = "CONNECTING"
        bot_state.log(f"Account {row.get('uid') or row.get('token', '')[:10]} {'paused' if now_paused else 'resumed'}.", "warning" if now_paused else "success", row.get("uid"), owner_username=row.get("owner_username", user.get("username", ADMIN_USERNAME)))
        return web.json_response({"status": "ok", "is_paused": now_paused})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Pause update failed: {type(exc).__name__}"}, status=400)


async def handle_restart_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "") or "").strip() if isinstance(data, dict) else ""
        user = request.get("current_user") or {}
        if not uid:
            return web.json_response({"status": "error", "error": "Account identifier required"}, status=400)
        row, all_records = await _find_owned_record(user, uid)
        if row is None:
            return web.json_response({"status": "error", "error": "Account not found in your portal"}, status=404)
        await _stop_record_workers([row])
        row["is_paused"] = False
        _save_all_account_files(all_records)
        for key in _worker_keys_for_record(row):
            account = bot_state.accounts.get(key) or bot_state.get_account(key, owner_username=row.get("owner_username"))
            if account is not None:
                account["status"] = "CONNECTING"
        started = await _start_record_workers([row])
        bot_state.log(f"Account {row.get('uid') or row.get('token', '')[:10]} restarted.", "info", row.get("uid"), owner_username=row.get("owner_username", user.get("username", ADMIN_USERNAME)))
        return web.json_response({"status": "ok", "started": bool(started)})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Restart failed: {type(exc).__name__}"}, status=400)


async def handle_pause_all_accounts(request: web.Request) -> web.Response:
    try:
        user = request.get("current_user") or {}
        all_records = await _load_accounts_file()
        owned = _visible_records(user, all_records)
        if not owned:
            return web.json_response({"status": "ok", "all_paused": False, "count": 0})
        currently_all_paused = all(bool(row.get("is_paused")) for row in owned)
        should_pause = not currently_all_paused
        for row in owned:
            row["is_paused"] = should_pause
        _save_all_account_files(all_records)
        if should_pause:
            await _stop_record_workers(owned)
        else:
            await _start_record_workers(owned)
            for row in owned:
                for key in _worker_keys_for_record(row):
                    account = bot_state.accounts.get(key) or bot_state.get_account(key, owner_username=row.get("owner_username"))
                    if account is not None:
                        account["status"] = "CONNECTING"
        bot_state.log(f"All {len(owned)} account(s) in this portal {'paused' if should_pause else 'resumed'}.", "warning" if should_pause else "success", owner_username=user.get("username", ADMIN_USERNAME))
        return web.json_response({"status": "ok", "all_paused": should_pause, "count": len(owned)})
    except Exception as exc:
        return web.json_response({"status": "error", "error": f"Pause-all failed: {type(exc).__name__}"}, status=400)


async def create_dashboard_app() -> web.Application:
    _ensure_account_storage_split()
    app = web.Application(middlewares=[auth_middleware])
    app.cleanup_ctx.append(_ban_checker_context)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/import", handle_batch_import)
    app.router.add_post("/api/account/delete-all", handle_delete_all_accounts)
    app.router.add_post("/api/logs/clear", handle_clear_logs)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/mode", handle_mode_switch)
    app.router.add_post("/api/account/pause", handle_pause_account)
    app.router.add_post("/api/account/restart", handle_restart_account)
    app.router.add_post("/api/account/pause_all", handle_pause_all_accounts)
    return app


async def start_web_dashboard(host: str = "0.0.0.0", port: int = 5000):
    app = await create_dashboard_app()
    configured_volume = _persistent_storage_available()
    print(f"[STORAGE] Data directory: {DATA_DIR}")
    if not configured_volume and (os.environ.get("RAILWAY_SERVICE_NAME") or os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RENDER")):
        print("[STORAGE] WARNING: no mounted persistent volume detected for the data directory. Account/user JSON files may disappear after a redeploy. On Railway, attach a volume mounted at /data; on other hosts set ROCKS_DATA_DIR to the actual persistent-disk mount path.")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"\033[92m[+] Web Dashboard running on http://localhost:{port}\033[0m")
