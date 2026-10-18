# -*- coding: utf-8 -*-
#MADE BY SHADOW GAMERZ DON'T CHANGE MY CREDIT TEL @ShadowGMRFF
import asyncio
import json
import os
import time
import secrets
import threading
import requests
import urllib3
from typing import Dict, List, Any, Optional
from aiohttp import web
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ═══════════════════════════════════════════════════════════════
#  OWNER CREDENTIALS (hardcoded)
# ═══════════════════════════════════════════════════════════════
ADMIN_USER = "ShadowGMRFF"
ADMIN_PASS = "ShadowGMRFF"

# In-memory sessions (24h TTL)
_SESSIONS: Dict[str, Dict[str, Any]] = {}
_SESSION_TTL = 86400
_SESSIONS_LOCK = threading.Lock()


# ═══════════════════════════════════════════════════════════════
#  EXP TABLE + LEVEL PROGRESS
# ═══════════════════════════════════════════════════════════════
EXP_TABLE: Dict[int, int] = {
    1: 0, 2: 48, 3: 202, 4: 544, 5: 1012, 6: 1844, 7: 2792, 8: 3800,
    9: 4870, 10: 6004, 11: 7192, 12: 8448, 13: 9760, 14: 11140, 15: 12566,
    16: 14060, 17: 15610, 18: 17224, 19: 18902, 20: 20632, 21: 22424, 22: 24278,
    23: 26192, 24: 28166, 25: 30200, 26: 32294, 27: 34448, 28: 37804, 29: 41274,
    30: 44870, 31: 48582, 32: 53394, 33: 58566, 34: 64096, 35: 69994, 36: 76460,
    37: 83506, 38: 91128, 39: 99322, 40: 108092, 41: 120144, 42: 133266, 43: 147472,
    44: 162760, 45: 179126, 46: 196572, 47: 215368, 48: 235516, 49: 257010, 50: 279860,
    51: 304056, 52: 348318, 53: 394982, 54: 444044, 55: 495508, 56: 549364, 57: 633756,
    58: 721744, 59: 813336, 60: 908522, 61: 1041438, 62: 1180352, 63: 1325266,
    64: 1476184, 65: 1634300, 66: 1840946, 67: 2056594, 68: 2281242, 69: 2514880,
    70: 2757530, 71: 3059506, 72: 3372284, 73: 3699456, 74: 4041030, 75: 4397002,
    76: 4829104, 77: 5282204, 78: 5756304, 79: 6251408, 80: 6776502, 81: 7381324,
    82: 8043154, 83: 8752982, 84: 9510808, 85: 10316338, 86: 11277190, 87: 12291748,
    88: 13360304, 89: 14482858, 90: 15659418, 91: 17026708, 92: 18453950, 93: 19941280,
    94: 21488570, 95: 23095858, 96: 24763138, 97: 26490428, 98: 28378704, 99: 30124996,
    100: 32032884,
}


def calculate_level_progress(level: int, current_exp: int) -> Dict[str, Any]:
    level = max(1, level)
    next_level = min(100, level + 1)
    base_exp = EXP_TABLE.get(level, 0)
    target_exp = EXP_TABLE.get(next_level, base_exp + 50000)

    needed_for_level = max(1, target_exp - base_exp)
    earned_in_level = max(0, current_exp - base_exp)
    remaining_exp = max(0, target_exp - current_exp)
    progress_pct = min(100.0, max(0.0, (earned_in_level / needed_for_level) * 100.0))

    return {
        "next_level": next_level,
        "base_exp": base_exp,
        "target_exp": target_exp,
        "needed_for_level": needed_for_level,
        "earned_in_level": earned_in_level,
        "remaining_exp": remaining_exp,
        "progress_pct": round(progress_pct, 1),
    }


# ═══════════════════════════════════════════════════════════════
#  BOT STATE (shared with best.py)
# ═══════════════════════════════════════════════════════════════
class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 300
        self.total_matches = 0
        self.total_matches_started = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_token_map: Dict[str, str] = {}
        self.auth_to_game_id: Dict[str, str] = {}
        self.game_to_auth_id: Dict[str, str] = {}
        self.paused_accounts: set = set()
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.active_writers: Dict[str, set] = {}
        self._lock = threading.Lock()

        # LIVE INFO caches
        self.open_ids: Dict[str, str] = {}
        self.access_tokens: Dict[str, str] = {}

        # 🔥 PER-ACCOUNT MODE overrides (BR / LW / AUTO)
        self.account_modes: Dict[str, str] = {}

        # 🆕 v6.1: RISK + SPEED state
        self.bot_mode = "AUTO"       # Global match mode
        self.bot_risk_mode = "SAFE"  # Global risk mode
        self.global_speed = 1.0      # Global speed multiplier
        self.account_speed_map: Dict[str, float] = {}  # Per-account speed

    # ─── WRITER MANAGEMENT ───
    def register_writer(self, uid: str, writer):
        uid_str = str(uid)
        with self._lock:
            if uid_str not in self.active_writers:
                self.active_writers[uid_str] = set()
            self.active_writers[uid_str].add(writer)

    def unregister_writer(self, uid: str, writer):
        uid_str = str(uid)
        with self._lock:
            if uid_str in self.active_writers:
                self.active_writers[uid_str].discard(writer)
                if not self.active_writers[uid_str]:
                    self.active_writers.pop(uid_str, None)

    def close_writers_for_account(self, uid: str):
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            candidates.add(str(self.game_to_auth_id[uid_str]))
        if uid_str in self.account_token_map:
            mapped = self.account_token_map[uid_str]
            candidates.add(str(mapped))
            candidates.add(str(mapped)[:16])

        for c in list(candidates):
            with self._lock:
                writers = list(self.active_writers.get(c, []))
            for w in writers:
                try:
                    if hasattr(w, "close"):
                        if hasattr(w, "is_closing"):
                            if not w.is_closing():
                                w.close()
                        else:
                            w.close()
                except Exception:
                    pass
            with self._lock:
                self.active_writers.pop(c, None)

    # ─── LOGGING ───
    def log(self, message: str, level: str = "info", uid: Optional[str] = None):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "uid": str(uid) if uid else None,
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

    # ─── 🎮 PER-ACCOUNT MODE ───
    def set_account_mode(self, uid: str, mode: str) -> str:
        uid_str = str(uid).strip()
        mode = str(mode).upper()
        if mode not in ("BR", "LW", "AUTO"):
            mode = "AUTO"
        if uid_str:
            self.account_modes[uid_str] = mode
        return mode

    def get_account_mode_override(self, uid: str) -> Optional[str]:
        uid_str = str(uid).strip()
        if not uid_str:
            return None
        if uid_str in self.account_modes:
            return self.account_modes[uid_str]
        aliases = []
        if uid_str in self.auth_to_game_id:
            aliases.append(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            aliases.append(str(self.game_to_auth_id[uid_str]))
        for a in aliases:
            if a in self.account_modes:
                return self.account_modes[a]
        return None

    def remove_account_mode(self, uid: str) -> bool:
        uid_str = str(uid).strip()
        removed = False
        if uid_str in self.account_modes:
            del self.account_modes[uid_str]
            removed = True
        aliases = []
        if uid_str in self.auth_to_game_id:
            aliases.append(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            aliases.append(str(self.game_to_auth_id[uid_str]))
        for a in aliases:
            if a in self.account_modes:
                del self.account_modes[a]
                removed = True
        return removed

    # ─── ACCOUNT REGISTRATION ───
    def register_account(self, uid: str, nickname: str, region: str, level: int,
                         exp: int, likes: int = 0, token: Optional[str] = None,
                         auth_uid: Optional[str] = None):
        uid_str = str(uid)
        auth_uid_str = str(auth_uid) if auth_uid else self.game_to_auth_id.get(uid_str, "")

        if auth_uid_str:
            self.auth_to_game_id[auth_uid_str] = uid_str
            self.game_to_auth_id[uid_str] = auth_uid_str
            self.account_token_map[auth_uid_str] = uid_str
            self.account_token_map[uid_str] = auth_uid_str

        if token:
            self.account_token_map[uid_str] = token
            self.account_token_map[token[:16]] = uid_str
            if auth_uid_str:
                self.account_token_map[auth_uid_str] = token

        prog = calculate_level_progress(level or 1, exp)
        lvl_val = level or 1
        acc_mode = "BR" if lvl_val < 3 else "LONE_WOLF"
        acc_mode_label = "Battle Royale (Lvl < 3)" if lvl_val < 3 else "Lone Wolf (Lvl 3+)"

        if uid_str not in self.accounts:
            self.accounts[uid_str] = {
                "uid": uid_str,
                "auth_uid": auth_uid_str or "",
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "BD",
                "level": lvl_val,
                "next_level": prog["next_level"],
                "mode": acc_mode,
                "mode_label": acc_mode_label,
                "initial_level": lvl_val,
                "initial_exp": exp,
                "current_exp": exp,
                "gained_exp": 0,
                "gained_levels": 0,
                "remaining_exp": prog["remaining_exp"],
                "target_exp": prog["target_exp"],
                "needed_for_level": prog["needed_for_level"],
                "earned_in_level": prog["earned_in_level"],
                "progress_pct": prog["progress_pct"],
                "likes": likes or 0,
                "status": "PAUSED" if self.is_paused(uid_str) else "ONLINE",
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "token": token or "",
                "start_time": time.time(),
                "is_paused": self.is_paused(uid_str),
                "paused_at": time.time() if self.is_paused(uid_str) else None,
                "total_pause_duration": 0.0,
                "last_updated": time.strftime("%H:%M:%S"),
                "live_name": nickname or f"Player_{uid_str[:6]}",
                "live_level": lvl_val,
                "live_uid": uid_str,
                "live_region": region or "BD",
            }
        else:
            acc = self.accounts[uid_str]
            if auth_uid_str:
                acc["auth_uid"] = auth_uid_str
            if nickname:
                acc["nickname"] = nickname
                acc["live_name"] = nickname
            if region:
                acc["region"] = region
                acc["live_region"] = region
            if level:
                acc["level"] = level
                acc["live_level"] = level
                acc["gained_levels"] = max(0, level - acc.get("initial_level", level))
            if token:
                acc["token"] = token
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["likes"] = likes
            if not acc.get("is_paused"):
                acc["status"] = "ONLINE"
            acc["last_updated"] = time.strftime("%H:%M:%S")

        self.recalc_totals()

    def update_live(self, uid: str, name: str = None, level: int = None, live_uid: str = None):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        acc = self.accounts[uid_str]
        if name:
            acc["live_name"] = name
        if level:
            acc["live_level"] = level
            acc["level"] = level
            acc["gained_levels"] = max(0, level - acc.get("initial_level", level))
        if live_uid:
            acc["live_uid"] = str(live_uid)
        acc["last_updated"] = time.strftime("%H:%M:%S")
        self.recalc_totals()

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        acc = self.accounts[uid_str]
        old_exp = acc["current_exp"]
        old_level = acc.get("level", 1)
        acc["current_exp"] = current_exp
        if level is not None and level > 0:
            acc["level"] = level
            acc["live_level"] = level
            acc["gained_levels"] = max(0, level - acc.get("initial_level", level))
        acc["gained_exp"] = max(0, current_exp - acc["initial_exp"])

        current_lvl = acc["level"]
        acc["mode"] = "BR" if current_lvl < 3 else "LONE_WOLF"
        acc["mode_label"] = "Battle Royale (Lvl < 3)" if current_lvl < 3 else "Lone Wolf (Lvl 3+)"

        prog = calculate_level_progress(acc["level"], current_exp)
        acc["next_level"] = prog["next_level"]
        acc["remaining_exp"] = prog["remaining_exp"]
        acc["target_exp"] = prog["target_exp"]
        acc["needed_for_level"] = prog["needed_for_level"]
        acc["earned_in_level"] = prog["earned_in_level"]
        acc["progress_pct"] = prog["progress_pct"]
        acc["last_updated"] = time.strftime("%H:%M:%S")

        if old_level < 3 and current_lvl >= 3:
            self.log(
                f"🎉 LEVEL UP! UID {uid_str} ({acc['nickname']}) → Lvl {current_lvl}! BR → LONE WOLF",
                "success", uid_str,
            )

        diff = current_exp - old_exp
        if diff > 0:
            self.log(
                f"★ UID {uid_str} ({acc['nickname']}) +{diff:,} EXP | Lvl {acc['level']} "
                f"[{acc['mode']}] ({prog['progress_pct']}% - {prog['remaining_exp']:,} to Lvl {prog['next_level']})",
                "success", uid_str,
            )
        self.recalc_totals()

    def get_account_level(self, uid: str) -> int:
        uid_str = str(uid)
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped and mapped in self.accounts:
                acc = self.accounts[mapped]
        if acc:
            return int(acc.get("level", 1) or 1)
        return 1

    def get_account_mode(self, uid: str) -> str:
        return "BR" if self.get_account_level(uid) < 3 else "LONE_WOLF"

    def get_account_uptime(self, uid_str: str) -> int:
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped and mapped in self.accounts:
                acc = self.accounts[mapped]
        if not acc:
            return 0
        start_t = acc.get("start_time", time.time())
        total_pause = acc.get("total_pause_duration", 0.0)
        if acc.get("is_paused") and acc.get("paused_at"):
            return max(0, int(acc["paused_at"] - start_t - total_pause))
        return max(0, int(time.time() - start_t - total_pause))

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            self.accounts[uid_str]["status"] = status
            if active_matches is not None:
                self.accounts[uid_str]["active_matches"] = active_matches
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match_started(self):
        self.total_matches_started += 1

    def increment_match(self, uid: str):
        uid_str = str(uid)
        self.total_matches += 1
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_match_time"] = time.strftime("%H:%M:%S")
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")
            self.log(
                f"⚔ Match #{self.accounts[uid_str]['matches_played']} finished "
                f"for {self.accounts[uid_str]['nickname']} ({uid_str})",
                "info", uid_str,
            )

    def recalc_totals(self):
        self.total_gained_exp = sum(acc.get("gained_exp", 0) for acc in self.accounts.values())

    # ─── PAUSE / RESUME ───
    def is_paused(self, uid: str) -> bool:
        uid_str = str(uid)
        if uid_str in self.paused_accounts:
            return True
        game_id = self.auth_to_game_id.get(uid_str)
        if game_id and game_id in self.paused_accounts:
            return True
        auth_uid = self.game_to_auth_id.get(uid_str)
        if auth_uid and auth_uid in self.paused_accounts:
            return True
        acc = self.accounts.get(uid_str) or (self.accounts.get(game_id) if game_id else None)
        if acc and acc.get("is_paused"):
            return True
        return False

    def toggle_pause(self, uid: str) -> bool:
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(self.auth_to_game_id[uid_str])
        if uid_str in self.game_to_auth_id:
            candidates.add(self.game_to_auth_id[uid_str])

        target_acc = None
        target_key = uid_str
        for c in candidates:
            if c in self.accounts:
                target_acc = self.accounts[c]
                target_key = c
                break

        is_now_paused = not self.is_paused(uid_str)

        if is_now_paused:
            for c in candidates:
                self.paused_accounts.add(c)
                self.close_writers_for_account(c)
            if target_acc:
                target_acc["is_paused"] = True
                target_acc["paused_at"] = time.time()
                target_acc["status"] = "PAUSED"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"⏸ UID {target_key} ({nick}) PAUSED", "warning", target_key)
            cb = self.refresh_callbacks.get("on_pause_toggle")
            if cb:
                try:
                    asyncio.create_task(cb(target_key, True))
                except Exception:
                    pass
        else:
            for c in candidates:
                self.paused_accounts.discard(c)
            if target_acc:
                target_acc["is_paused"] = False
                if target_acc.get("paused_at"):
                    pause_dur = time.time() - target_acc["paused_at"]
                    target_acc["total_pause_duration"] = (
                        target_acc.get("total_pause_duration", 0.0) + pause_dur
                    )
                    target_acc["paused_at"] = None
                target_acc["status"] = "ONLINE"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"▶ UID {target_key} ({nick}) RESUMED", "success", target_key)
            cb = self.refresh_callbacks.get("on_pause_toggle")
            if cb:
                try:
                    asyncio.create_task(cb(target_key, False))
                except Exception:
                    pass

        return is_now_paused

    def toggle_pause_all(self) -> bool:
        any_active = any(not self.is_paused(k) for k in self.accounts.keys())
        for k in list(self.accounts.keys()):
            current_paused = self.is_paused(k)
            if any_active and not current_paused:
                self.toggle_pause(k)
            elif not any_active and current_paused:
                self.toggle_pause(k)
        return any_active

    # ─── FULL ACCOUNT REMOVAL ───
    def remove_account(self, uid: str) -> bool:
        uid = str(uid).strip()
        if not uid:
            return False

        worker_keys_to_cancel = set()

        if uid in self.account_workers:
            worker_keys_to_cancel.add(uid)

        for ck, cred in list(self.account_credentials.items()):
            try:
                acc_id = str(cred.get("account_id", ""))
                auth_uid = str(cred.get("auth_uid", ""))
                auth_token = cred.get("auth_token", "")
                if acc_id == uid or auth_uid == uid or str(ck) == uid:
                    if auth_uid and auth_uid in self.account_workers:
                        worker_keys_to_cancel.add(auth_uid)
                    if auth_token:
                        tk = auth_token[:10]
                        if tk in self.account_workers:
                            worker_keys_to_cancel.add(tk)
            except Exception:
                pass

        for k, v in list(self.access_tokens.items()):
            if str(k) == uid:
                if v and v[:10] in self.account_workers:
                    worker_keys_to_cancel.add(v[:10])

        for wk in worker_keys_to_cancel:
            try:
                task = self.account_workers.get(wk)
                if task and not task.done():
                    task.cancel()
            except Exception:
                pass
            self.account_workers.pop(wk, None)

        self.accounts.pop(uid, None)
        self.open_ids.pop(uid, None)
        self.access_tokens.pop(uid, None)
        self.account_modes.pop(uid, None)
        self.account_speed_map.pop(uid, None)

        for ck in list(self.account_credentials.keys()):
            try:
                cred = self.account_credentials[ck]
                acc_id = str(cred.get("account_id", ""))
                auth_uid = str(cred.get("auth_uid", ""))
                if acc_id == uid or auth_uid == uid or str(ck) == uid:
                    del self.account_credentials[ck]
            except Exception:
                pass

        try:
            self.log(
                f"🗑️ [DELETE] Account {uid} + {len(worker_keys_to_cancel)} worker(s) cancelled",
                "warning", uid,
            )
        except Exception:
            pass
        return True


bot_state = BotState()


# ═══════════════════════════════════════════════════════════════
#  FF LIVE INFO FETCH
# ═══════════════════════════════════════════════════════════════
_AES_KEY = b"Yg&tc%DEuh6%Zc^8"
_AES_IV  = b"6oyZDr22E3ychjM%"
_FF_SERVER = "https://loginbp.ppmainecoonghj.com"
_FF_RELEASE = "OB55"


def _vint(v: int) -> bytes:
    out = []
    while v > 0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _proto_str(field: int, val: str) -> bytes:
    b = val.encode()
    return _vint((field << 3) | 2) + _vint(len(b)) + b


def _read_varint(b: bytes, p: int):
    r = 0
    s = 0
    while p < len(b):
        x = b[p]
        p += 1
        r |= (x & 0x7F) << s
        if not (x & 0x80):
            return r, p
        s += 7
    return r, p


def _decode_proto(b: bytes) -> dict:
    p = 0
    r = {}
    while p < len(b):
        try:
            t, p = _read_varint(b, p)
            fid = t >> 3
            wt = t & 7
            if wt == 0:
                v, p = _read_varint(b, p)
                r[fid] = v
            elif wt == 2:
                l, p = _read_varint(b, p)
                vb = b[p:p + l]
                p += l
                try:
                    r[fid] = vb.decode("utf-8")
                except Exception:
                    r[fid] = vb
            else:
                break
        except Exception:
            break
    return r


def _gen_access_token(uid: str, password: str):
    url = "https://ffmconnect.live.gop.garenanow.com/oauth/guest/token/grant"
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067",
    }
    h = {
        "User-Agent": "GarenaMSDK/4.0.19P9(HGAFSDB ;Android 9;en;US;)",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        r = requests.post(url, data=data, headers=h, timeout=12, verify=False)
        if r.status_code == 200:
            d = r.json()
            return d.get("access_token"), d.get("open_id")
    except Exception:
        pass
    return None, None


def _access_to_open_id(at: str):
    url = f"https://ffmconnect.live.gop.garenanow.com/oauth/token/inspect?token={at}"
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "GarenaMSDK/4.0.19P9"},
            timeout=12,
            verify=False,
        )
        if r.status_code == 200:
            return r.json().get("open_id")
    except Exception:
        pass
    return None


def _fetch_brief_info(open_id: str):
    url = f"{_FF_SERVER}/GetAccountBriefInfoBeforeLogin"
    h = {
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "X-Ga-Sv": str(int(time.time())),
        "X-Ga": "v1 1",
        "Releaseversion": _FF_RELEASE,
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Host": "loginbp.ppmainecoonghj.com",
    }
    try:
        body = _proto_str(1, open_id)
        enc = AES.new(_AES_KEY, AES.MODE_CBC, _AES_IV).encrypt(pad(body, 16))
        r = requests.post(url, headers=h, data=enc, timeout=12, verify=False)
        if r.status_code != 200:
            return None
        d = _decode_proto(r.content)
        return {
            "uid": str(d.get(1, "")),
            "name": str(d.get(2, "")),
            "level": int(d.get(5, 0) or 0),
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
#  🎮 GLOBAL MATCH MODE MANAGER (BR / LW / AUTO)
# ═══════════════════════════════════════════════════════════════
MATCH_MODE_FILE = "match_mode.json"

_VALID_MODES = ("BR", "LW", "AUTO")
_mode_lock = threading.Lock()


def _read_match_mode() -> str:
    try:
        if os.path.exists(MATCH_MODE_FILE):
            with open(MATCH_MODE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                mode = str(data.get("mode", "AUTO")).upper()
                if mode in _VALID_MODES:
                    return mode
    except Exception:
        pass
    return "AUTO"


def _write_match_mode(mode: str) -> str:
    mode = str(mode).upper()
    if mode not in _VALID_MODES:
        mode = "AUTO"
    with _mode_lock:
        try:
            tmp = MATCH_MODE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {"mode": mode, "updated": time.time(), "updated_by": ADMIN_USER},
                    f, indent=2,
                )
            os.replace(tmp, MATCH_MODE_FILE)
        except Exception as e:
            print(f"[MODE] Failed to write match mode: {e}")
    return mode


# ═══════════════════════════════════════════════════════════════
#  🆕 v6.1: RISK MODE MANAGER (SAFE / RISK)
# ═══════════════════════════════════════════════════════════════
RISK_MODE_FILE = "risk_mode.json"

_VALID_RISK = ("SAFE", "RISK")
_risk_lock = threading.Lock()


def _read_risk_mode() -> str:
    try:
        if os.path.exists(RISK_MODE_FILE):
            with open(RISK_MODE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                m = str(d.get("mode", "SAFE")).upper()
                if m in _VALID_RISK:
                    return m
    except Exception:
        pass
    return "SAFE"


def _write_risk_mode(mode: str) -> str:
    mode = str(mode).upper()
    if mode not in _VALID_RISK:
        mode = "SAFE"
    with _risk_lock:
        try:
            tmp = RISK_MODE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {"mode": mode, "updated": time.time(), "updated_by": ADMIN_USER},
                    f, indent=2,
                )
            os.replace(tmp, RISK_MODE_FILE)
        except Exception as e:
            print(f"[RISK] Failed to write risk mode: {e}")
    return mode


# ═══════════════════════════════════════════════════════════════
#  🆕 v6.1: SPEED CONFIG MANAGER (Global + Per-account)
# ═══════════════════════════════════════════════════════════════
SPEED_FILE = "speed_config.json"

_speed_lock = threading.Lock()
_MIN_SPEED = 0.25
_MAX_SPEED = 5.0


def _read_speed_config() -> dict:
    """Returns {"global": float, "accounts": {uid: float}}"""
    try:
        if os.path.exists(SPEED_FILE):
            with open(SPEED_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                g = float(d.get("global", 1.0))
                a = d.get("accounts", {}) or {}
                g = max(_MIN_SPEED, min(g, _MAX_SPEED))
                a = {str(k): max(_MIN_SPEED, min(float(v), _MAX_SPEED)) for k, v in a.items()}
                return {"global": g, "accounts": a}
    except Exception:
        pass
    return {"global": 1.0, "accounts": {}}


def _write_speed_config(cfg: dict) -> dict:
    with _speed_lock:
        try:
            tmp = SPEED_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            os.replace(tmp, SPEED_FILE)
        except Exception as e:
            print(f"[SPEED] write failed: {e}")
    return cfg


# ═══════════════════════════════════════════════════════════════
#  AUTH HELPERS
# ═══════════════════════════════════════════════════════════════
def _create_session() -> str:
    t = secrets.token_urlsafe(32)
    with _SESSIONS_LOCK:
        _SESSIONS[t] = {"created": time.time(), "user": ADMIN_USER}
    return t


def _valid_session(token: Optional[str]) -> bool:
    if not token:
        return False
    with _SESSIONS_LOCK:
        s = _SESSIONS.get(token)
        if not s:
            return False
        if time.time() - s["created"] > _SESSION_TTL:
            _SESSIONS.pop(token, None)
            return False
        return True


def _require_auth(request: web.Request) -> bool:
    return _valid_session(request.cookies.get("session"))


# ═══════════════════════════════════════════════════════════════
#  PATHS
# ═══════════════════════════════════════════════════════════════
_HERE = os.path.dirname(os.path.abspath(__file__))
TPL_INDEX = os.path.join(_HERE, "templates", "index.html")
TPL_LOGIN = os.path.join(_HERE, "templates", "login.html")

ACCOUNTS_FILE = "accounts.json"
TOKENS_FILE = "access_token.json"
TOKEN_CACHE_FILE = "token_cache.json"
DEVICES_FILE = "devices.json"


# ═══════════════════════════════════════════════════════════════
#  FILE HELPERS
# ═══════════════════════════════════════════════════════════════
_file_lock = threading.Lock()


def _read_json(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_json(path: str, data: List[Dict]):
    with _file_lock:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)


def _remove_from_token_cache(uid: str):
    if not os.path.exists(TOKEN_CACHE_FILE):
        return
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return
        cache = json.loads(content)
        if not isinstance(cache, dict):
            return

        keys_to_delete = []
        for k in list(cache.keys()):
            if str(k) == uid:
                keys_to_delete.append(k)
                continue
            try:
                entry = cache[k]
                if isinstance(entry, dict):
                    if str(entry.get("account_id", "")) == uid:
                        keys_to_delete.append(k)
                    elif str(entry.get("auth_uid", "")) == uid:
                        keys_to_delete.append(k)
            except Exception:
                pass

        for k in keys_to_delete:
            del cache[k]

        if keys_to_delete:
            tmp = TOKEN_CACHE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
            os.replace(tmp, TOKEN_CACHE_FILE)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
#  PAGE HANDLERS
# ═══════════════════════════════════════════════════════════════
async def handle_login_page(request: web.Request) -> web.Response:
    # index.html already has built-in login screen — serve it
    if os.path.exists(TPL_INDEX):
        with open(TPL_INDEX, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html", charset="utf-8")
    if os.path.exists(TPL_LOGIN):
        with open(TPL_LOGIN, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html", charset="utf-8")
    return web.Response(text="<h1>templates/index.html not found!</h1>", content_type="text/html")


async def handle_index(request: web.Request) -> web.Response:
    # index.html has client-side auth check — always serve it
    if os.path.exists(TPL_INDEX):
        with open(TPL_INDEX, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html", charset="utf-8")
    return web.Response(text="<h1>templates/index.html not found!</h1>", content_type="text/html")


# ═══════════════════════════════════════════════════════════════
#  AUTH API
# ═══════════════════════════════════════════════════════════════
async def handle_login(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        user = str(data.get("username", "")).strip()
        pwd = str(data.get("password", "")).strip()

        if user.lower() != ADMIN_USER.lower() or pwd != ADMIN_PASS:
            bot_state.log(f"Failed login attempt: {user}", "warning")
            return web.json_response({"status": "error", "error": "Invalid credentials"})

        token = _create_session()
        resp = web.json_response({"status": "ok"})
        resp.set_cookie("session", token, max_age=_SESSION_TTL, httponly=True, samesite="Lax")
        bot_state.log(f"Owner logged in: {user}", "success")
        return resp
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_logout(request: web.Request) -> web.Response:
    t = request.cookies.get("session")
    if t:
        with _SESSIONS_LOCK:
            _SESSIONS.pop(t, None)
    resp = web.json_response({"status": "ok"})
    resp.del_cookie("session")
    return resp


async def handle_check_auth(request: web.Request) -> web.Response:
    return web.json_response({"authenticated": _require_auth(request)})


# ═══════════════════════════════════════════════════════════════
#  STATS API (updated with risk_mode + speed)
# ═══════════════════════════════════════════════════════════════
async def handle_get_stats(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)

    accounts_data = list(bot_state.accounts.values())
    accounts_data.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)

    uptime_sec = max(1, int(time.time() - bot_state.start_time))
    total_gained = bot_state.total_gained_exp
    exp_per_hour = int((total_gained / uptime_sec) * 3600)
    total_active_matches = sum(acc.get("active_matches", 0) for acc in accounts_data)
    total_levels = sum(a.get("gained_levels", 0) for a in accounts_data)

    speed_cfg = _read_speed_config()

    for acc in accounts_data:
        uid_k = str(acc.get("uid", ""))
        acc["uptime_seconds"] = bot_state.get_account_uptime(uid_k)
        acc["is_paused"] = bot_state.is_paused(uid_k)
        # Attach per-account mode
        try:
            ov = bot_state.get_account_mode_override(uid_k)
            acc["acc_mode"] = ov if ov else ""
        except Exception:
            acc["acc_mode"] = ""
        # Attach per-account speed
        try:
            spd = speed_cfg["accounts"].get(uid_k, speed_cfg["global"])
            acc["speed"] = float(spd)
        except Exception:
            acc["speed"] = 1.0

    return web.json_response({
        "total_accounts": len(bot_state.accounts),
        "total_matches": bot_state.total_matches,
        "total_matches_started": bot_state.total_matches_started,
        "total_active_matches": total_active_matches,
        "total_gained_exp": total_gained,
        "total_gained_levels": total_levels,
        "exp_per_hour": exp_per_hour,
        "accounts": accounts_data,
        "logs": bot_state.logs[-80:],
        "uptime": uptime_sec,
        "match_mode": _read_match_mode(),
        "risk_mode": _read_risk_mode(),
        "global_speed": speed_cfg["global"],
        "account_speeds": speed_cfg["accounts"],
        "account_modes": dict(bot_state.account_modes),
    })


# ═══════════════════════════════════════════════════════════════
#  🎮 GLOBAL MATCH MODE API
# ═══════════════════════════════════════════════════════════════
async def handle_get_match_mode(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    return web.json_response({
        "status": "ok",
        "mode": _read_match_mode(),
        "modes": list(_VALID_MODES),
    })


async def handle_set_match_mode(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        new_mode = str(data.get("mode", "AUTO")).upper()
        saved = _write_match_mode(new_mode)

        desc = {
            "BR": "⚔️ BR Only — all accounts search Battle Royale only",
            "LW": "🐺 Lone Wolf Only — all accounts search Lone Wolf only",
            "AUTO": "🔄 Auto — Level < 3 → BR | Level >= 3 → Lone Wolf",
        }.get(saved, saved)

        try:
            bot_state.bot_mode = saved
        except Exception:
            pass

        bot_state.log(f"🎮 Global match mode → {saved} ({desc})", "success")
        return web.json_response({"status": "ok", "mode": saved, "desc": desc})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  🆕 v6.1: RISK MODE API
# ═══════════════════════════════════════════════════════════════
async def handle_get_risk_mode(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    return web.json_response({
        "status": "ok",
        "mode": _read_risk_mode(),
        "modes": list(_VALID_RISK),
    })


async def handle_set_risk_mode(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        new_mode = _write_risk_mode(data.get("mode", "SAFE"))
        try:
            bot_state.bot_risk_mode = new_mode
        except Exception:
            pass
        bot_state.log(f"🎚️ Risk mode → {new_mode}", "success")

        # Notify bot via callback
        cb = bot_state.refresh_callbacks.get("on_risk_mode_change")
        if cb:
            try:
                asyncio.create_task(cb(new_mode))
            except Exception:
                pass

        return web.json_response({"status": "ok", "mode": new_mode})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  🆕 v6.1: SPEED API (Global + Per-account)
# ═══════════════════════════════════════════════════════════════
async def handle_get_speed(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    cfg = _read_speed_config()
    return web.json_response({
        "status": "ok",
        "global": cfg["global"],
        "accounts": cfg["accounts"],
    })


async def handle_set_global_speed(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        spd = float(data.get("speed", 1.0))
        spd = max(_MIN_SPEED, min(spd, _MAX_SPEED))

        cfg = _read_speed_config()
        cfg["global"] = spd
        _write_speed_config(cfg)

        try:
            bot_state.global_speed = spd
            if not hasattr(bot_state, "account_speed_map"):
                bot_state.account_speed_map = {}
        except Exception:
            pass

        bot_state.log(f"⚡ Global speed → {spd}x", "success")

        cb = bot_state.refresh_callbacks.get("on_speed_change")
        if cb:
            try:
                asyncio.create_task(cb({"speed": spd}))
            except Exception:
                pass

        return web.json_response({"status": "ok", "speed": spd})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_set_account_speed(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "uid required"})

        spd = float(data.get("speed", 1.0))
        spd = max(_MIN_SPEED, min(spd, _MAX_SPEED))

        cfg = _read_speed_config()
        cfg["accounts"][uid] = spd
        _write_speed_config(cfg)

        try:
            if not hasattr(bot_state, "account_speed_map"):
                bot_state.account_speed_map = {}
            bot_state.account_speed_map[uid] = spd
        except Exception:
            pass

        # Persist into accounts.json too
        try:
            accs = _read_json(ACCOUNTS_FILE)
            for a in accs:
                if str(a.get("uid", "")).strip() == uid:
                    a["speed"] = spd
                    break
            _write_json(ACCOUNTS_FILE, accs)
        except Exception:
            pass

        bot_state.log(f"⚡ Account {uid} speed → {spd}x", "success", uid)

        cb = bot_state.refresh_callbacks.get("on_speed_change")
        if cb:
            try:
                asyncio.create_task(cb({"uid": uid, "speed": spd}))
            except Exception:
                pass

        return web.json_response({"status": "ok", "speed": spd, "uid": uid})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  🔥 PER-ACCOUNT MODE API
# ═══════════════════════════════════════════════════════════════
async def handle_set_account_mode(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        mode = str(data.get("mode", "AUTO")).upper()

        if not uid:
            return web.json_response({"status": "error", "error": "UID required"})
        if mode not in _VALID_MODES:
            mode = "AUTO"

        bot_state.set_account_mode(uid, mode)

        # Update accounts.json
        existing = _read_json(ACCOUNTS_FILE)
        updated = False
        for a in existing:
            if str(a.get("uid", "")).strip() == uid:
                a["mode"] = mode
                updated = True
                break
        if updated:
            _write_json(ACCOUNTS_FILE, existing)

        bot_state.log(f"🎮 Account {uid} mode → {mode}", "success", uid)
        return web.json_response({"status": "ok", "mode": mode, "updated": updated})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_get_account_mode(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        uid = str(request.query.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "UID required"})
        ov = bot_state.get_account_mode_override(uid)
        return web.json_response({
            "status": "ok",
            "uid": uid,
            "mode": ov if ov else "AUTO",
            "has_override": ov is not None,
        })
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  LIVE INFO API
# ═══════════════════════════════════════════════════════════════
async def handle_live_refresh(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)

    open_ids = dict(bot_state.open_ids)
    if not open_ids:
        return web.json_response({
            "status": "ok", "updated": 0, "total": 0,
            "message": "No open_ids cached yet",
        })

    updated = 0
    loop = asyncio.get_event_loop()

    async def _fetch(uid: str, oid: str):
        nonlocal updated
        info = await loop.run_in_executor(None, _fetch_brief_info, oid)
        if info and info.get("level"):
            bot_state.update_live(uid, info.get("name"), info.get("level"), info.get("uid"))
            updated += 1

    await asyncio.gather(
        *[_fetch(u, o) for u, o in open_ids.items()],
        return_exceptions=True,
    )

    bot_state.log(f"Live refresh: {updated}/{len(open_ids)} updated", "success")
    return web.json_response({"status": "ok", "updated": updated, "total": len(open_ids)})


async def handle_live_one(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        oid = bot_state.open_ids.get(uid)
        if not oid:
            return web.json_response({"status": "error", "error": "open_id not cached for this UID"})

        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, _fetch_brief_info, oid)
        if not info:
            return web.json_response({"status": "error", "error": "fetch failed"})

        bot_state.update_live(uid, info.get("name"), info.get("level"), info.get("uid"))
        return web.json_response({"status": "ok", "info": info})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  ACCOUNTS API
# ═══════════════════════════════════════════════════════════════
async def handle_list_accounts(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    return web.json_response({"accounts": _read_json(ACCOUNTS_FILE)})


async def handle_add_account(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error", "error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        items = data.get("accounts") if isinstance(data.get("accounts"), list) else [data]

        existing = _read_json(ACCOUNTS_FILE)
        added = 0

        for item in items:
            uid = str(item.get("uid", "")).strip()
            pwd = str(item.get("password", "")).strip()
            at = str(item.get("token", "")).strip()
            if not uid and not at:
                continue
            if not at and not pwd:
                continue

            acc_mode = str(item.get("mode", "AUTO")).upper()
            if acc_mode not in _VALID_MODES:
                acc_mode = "AUTO"

            speed = float(item.get("speed", 1.0)) if item.get("speed") else 1.0
            speed = max(_MIN_SPEED, min(speed, _MAX_SPEED))

            # Cancel previous worker
            if uid and uid in bot_state.account_workers:
                try:
                    bot_state.account_workers[uid].cancel()
                except Exception:
                    pass
                bot_state.account_workers.pop(uid, None)

            # Fetch open_id for LIVE INFO
            if uid and pwd and not at:
                try:
                    loop = asyncio.get_event_loop()
                    _at, oid = await loop.run_in_executor(None, _gen_access_token, uid, pwd)
                    if oid:
                        bot_state.open_ids[uid] = oid
                        if _at:
                            bot_state.access_tokens[uid] = _at
                except Exception:
                    pass
            elif at:
                try:
                    loop = asyncio.get_event_loop()
                    oid = await loop.run_in_executor(None, _access_to_open_id, at)
                    if oid:
                        key = uid or oid
                        bot_state.open_ids[key] = oid
                        bot_state.access_tokens[key] = at
                except Exception:
                    pass

            try:
                bot_state.set_account_mode(uid or f"tok_{at[:16]}", acc_mode)
            except Exception:
                pass

            try:
                if not hasattr(bot_state, "account_speed_map"):
                    bot_state.account_speed_map = {}
                bot_state.account_speed_map[uid or f"tok_{at[:16]}"] = speed
            except Exception:
                pass

            existing = [a for a in existing if str(a.get("uid")) != uid and str(a.get("token", "")) != at]

            entry = {
                "uid": uid,
                "name": item.get("name", f"Player_{uid[:6]}" if uid else "TokenUser"),
                "region": item.get("region", "IND"),
                "mode": acc_mode,
                "speed": speed,
            }
            if at:
                entry["token"] = at
            else:
                entry["password"] = pwd

            existing.append(entry)
            added += 1

            callback_data = dict(item)
            callback_data["mode"] = acc_mode
            callback_data["speed"] = speed
            cb = bot_state.refresh_callbacks.get("on_account_added")
            if cb:
                asyncio.create_task(cb(callback_data))

        _write_json(ACCOUNTS_FILE, existing)
        bot_state.log(f"{added} account(s) uploaded", "success")
        return web.json_response({"status": "ok", "added": added})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_delete_account(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        req_uid = str(data.get("uid", "")).strip()
        req_auth_uid = str(data.get("auth_uid", "")).strip()

        if not req_uid and not req_auth_uid:
            return web.json_response({"status": "error", "error": "uid required"})

        candidate_ids = set()
        if req_uid:
            candidate_ids.add(req_uid)
        if req_auth_uid:
            candidate_ids.add(req_auth_uid)

        for cid in list(candidate_ids):
            if cid in bot_state.game_to_auth_id:
                candidate_ids.add(str(bot_state.game_to_auth_id[cid]))
            if cid in bot_state.auth_to_game_id:
                candidate_ids.add(str(bot_state.auth_to_game_id[cid]))

        target_tokens = set()
        for cid in list(candidate_ids):
            acc_info = bot_state.accounts.get(cid, {})
            if acc_info:
                if acc_info.get("auth_uid"):
                    candidate_ids.add(str(acc_info["auth_uid"]))
                if acc_info.get("uid"):
                    candidate_ids.add(str(acc_info["uid"]))
                t = acc_info.get("token") or acc_info.get("access_token")
                if t:
                    target_tokens.add(str(t))

        for cid in candidate_ids:
            bot_state.remove_account(cid)
            try:
                bot_state.remove_account_mode(cid)
            except Exception:
                pass

        # Clean accounts.json
        existing = _read_json(ACCOUNTS_FILE)
        new_existing = []
        for a in existing:
            a_uid = str(a.get("uid", "")).strip()
            a_acc_id = str(a.get("account_id", "")).strip()
            a_tok = str(a.get("token", "")).strip()
            if a_uid in candidate_ids or a_acc_id in candidate_ids or (a_tok and a_tok in target_tokens):
                continue
            new_existing.append(a)
        _write_json(ACCOUNTS_FILE, new_existing)

        # Clean token cache
        for cid in candidate_ids:
            _remove_from_token_cache(cid)

        # Clean devices.json
        if os.path.exists(DEVICES_FILE):
            try:
                with open(DEVICES_FILE, "r", encoding="utf-8") as f:
                    devices_data = json.load(f)
                dirty = False
                for dev_k in list(devices_data.keys()):
                    if str(dev_k) in candidate_ids:
                        del devices_data[dev_k]
                        dirty = True
                if dirty:
                    with open(DEVICES_FILE, "w", encoding="utf-8") as f:
                        json.dump(devices_data, f, indent=4)
            except Exception:
                pass

        # Cancel workers
        cancelled_keys = []
        for k, worker in list(bot_state.account_workers.items()):
            if str(k) in candidate_ids:
                try:
                    worker.cancel()
                except Exception:
                    pass
                cancelled_keys.append(k)
        for k in cancelled_keys:
            bot_state.account_workers.pop(k, None)

        for cid in candidate_ids:
            bot_state.close_writers_for_account(cid)

        cb = bot_state.refresh_callbacks.get("on_account_deleted")
        if cb:
            try:
                asyncio.create_task(cb(list(candidate_ids)))
            except Exception:
                pass

        bot_state.log(f"🗑️ Account {req_uid or req_auth_uid} fully removed", "warning", req_uid)
        return web.json_response({"status": "ok", "deleted": list(candidate_ids)})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_refresh_account(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        cb = bot_state.refresh_callbacks.get("on_refresh_account")
        if cb:
            asyncio.create_task(cb(uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_restart_account(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        cb = bot_state.refresh_callbacks.get("on_restart_account")
        if cb:
            asyncio.create_task(cb(uid))
        else:
            cb2 = bot_state.refresh_callbacks.get("on_refresh_account")
            if cb2:
                asyncio.create_task(cb2(uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_pause(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "UID required"})
        is_paused = bot_state.toggle_pause(uid)
        return web.json_response({"status": "ok", "is_paused": is_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_pause_all(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        paused_state = bot_state.toggle_pause_all()
        return web.json_response({"status": "ok", "all_paused": paused_state})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_clear_logs(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    bot_state.logs.clear()
    return web.json_response({"status": "ok"})


# ═══════════════════════════════════════════════════════════════
#  TOKENS API
# ═══════════════════════════════════════════════════════════════
async def handle_list_tokens(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    tokens = _read_json(TOKENS_FILE)
    safe = []
    for t in tokens:
        at = str(t.get("access_token", ""))
        safe.append({
            "uid": t.get("uid", ""),
            "guest_id": (str(t.get("guest_id", ""))[:12] + "...") if t.get("guest_id") else "",
            "access_token": (at[:20] + "...") if at else "",
            "saved_at": t.get("saved_at", ""),
            "mode": t.get("mode", "AUTO"),
            "_raw": at,
        })
    return web.json_response({"tokens": safe})


async def handle_add_token(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error", "error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        items = data.get("tokens") if isinstance(data.get("tokens"), list) else [data]

        existing = _read_json(TOKENS_FILE)
        added = 0

        for item in items:
            at = str(item.get("access_token", "")).strip()
            if not at:
                continue
            uid = str(item.get("uid", "")).strip()

            acc_mode = str(item.get("mode", "AUTO")).upper()
            if acc_mode not in _VALID_MODES:
                acc_mode = "AUTO"

            try:
                loop = asyncio.get_event_loop()
                oid = await loop.run_in_executor(None, _access_to_open_id, at)
                if oid:
                    key = uid or oid
                    bot_state.open_ids[key] = oid
                    bot_state.access_tokens[key] = at
                    if acc_mode:
                        bot_state.set_account_mode(key, acc_mode)
            except Exception:
                pass

            entry = {
                "uid": uid,
                "access_token": at,
                "guest_id": item.get("guest_id", ""),
                "expires_in": item.get("expires_in", 1296000),
                "token_type": item.get("token_type", "Bearer"),
                "refresh_token": item.get("refresh_token", ""),
                "saved_at": item.get("saved_at", time.strftime("%Y-%m-%d %H:%M:%S")),
                "mode": acc_mode,
            }
            existing = [t for t in existing if str(t.get("access_token")) != at]
            existing.append(entry)
            added += 1

            cb = bot_state.refresh_callbacks.get("on_account_added")
            if cb:
                asyncio.create_task(cb({"token": at, "mode": acc_mode}))

        _write_json(TOKENS_FILE, existing)
        bot_state.log(f"{added} token(s) uploaded", "success")
        return web.json_response({"status": "ok", "added": added})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_delete_token(request: web.Request) -> web.Response:
    if not _require_auth(request):
        return web.json_response({"status": "error"}, status=401)
    try:
        data = await request.json()
        at = str(data.get("access_token", "")).strip()

        _write_json(
            TOKENS_FILE,
            [t for t in _read_json(TOKENS_FILE) if str(t.get("access_token")) != at],
        )

        label = at[:10]
        if label in bot_state.account_workers:
            try:
                bot_state.account_workers[label].cancel()
            except Exception:
                pass
            del bot_state.account_workers[label]

        for k in list(bot_state.access_tokens.keys()):
            if bot_state.access_tokens[k] == at:
                bot_state.open_ids.pop(k, None)
                bot_state.access_tokens.pop(k, None)
                bot_state.accounts.pop(str(k), None)
                bot_state.account_modes.pop(str(k), None)
                bot_state.account_speed_map.pop(str(k), None)

        _remove_from_token_cache(f"tok_{at[:20]}")

        bot_state.log(f"🗑️ Token {label}... removed", "warning")
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  BACKGROUND: AUTO LIVE REFRESH (every 60s)
# ═══════════════════════════════════════════════════════════════
async def _auto_live_loop():
    while True:
        await asyncio.sleep(60)
        try:
            if not bot_state.open_ids:
                continue
            loop = asyncio.get_event_loop()

            async def _f(uid: str, oid: str):
                info = await loop.run_in_executor(None, _fetch_brief_info, oid)
                if info and info.get("level"):
                    bot_state.update_live(uid, info.get("name"), info.get("level"), info.get("uid"))

            await asyncio.gather(
                *[_f(u, o) for u, o in bot_state.open_ids.items()],
                return_exceptions=True,
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  SERVER START
# ═══════════════════════════════════════════════════════════════
async def start_web_dashboard(host: str = "0.0.0.0", port: int = 5000):
    app = web.Application(client_max_size=10 * 1024 * 1024)

    # Pages
    app.router.add_get("/login", handle_login_page)
    app.router.add_get("/", handle_index)

    # Auth
    app.router.add_post("/api/login", handle_login)
    app.router.add_post("/api/logout", handle_logout)
    app.router.add_get("/api/check-auth", handle_check_auth)

    # Stats
    app.router.add_get("/api/stats", handle_get_stats)

    # 🎮 Global Match Mode (BR / LW / AUTO)
    app.router.add_get("/api/match-mode", handle_get_match_mode)
    app.router.add_post("/api/match-mode", handle_set_match_mode)

    # 🆕 🎚️ Risk Mode (SAFE / RISK)
    app.router.add_get("/api/risk-mode", handle_get_risk_mode)
    app.router.add_post("/api/risk-mode", handle_set_risk_mode)

    # 🆕 ⚡ Speed (Global + Per-account)
    app.router.add_get("/api/speed", handle_get_speed)
    app.router.add_post("/api/speed/global", handle_set_global_speed)
    app.router.add_post("/api/speed/account", handle_set_account_speed)

    # 🔥 Per-Account Mode
    app.router.add_get("/api/account/mode", handle_get_account_mode)
    app.router.add_post("/api/account/mode", handle_set_account_mode)

    # Live info
    app.router.add_post("/api/live/refresh", handle_live_refresh)
    app.router.add_post("/api/live/one", handle_live_one)

    # Accounts
    app.router.add_get("/api/accounts/list", handle_list_accounts)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/restart", handle_restart_account)
    app.router.add_post("/api/account/pause", handle_toggle_pause)
    app.router.add_post("/api/account/pause_all", handle_toggle_pause_all)

    # Tokens
    app.router.add_get("/api/tokens/list", handle_list_tokens)
    app.router.add_post("/api/token/add", handle_add_token)
    app.router.add_post("/api/token/delete", handle_delete_token)

    # Logs
    app.router.add_post("/api/logs/clear", handle_clear_logs)

    # Background auto live refresh
    asyncio.create_task(_auto_live_loop())

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    current_mode = _read_match_mode()
    current_risk = _read_risk_mode()
    current_speed = _read_speed_config()["global"]
    print(f"\033[92m[+] 🔐 Owner Dashboard → http://0.0.0.0:{port}\033[0m")
    print(f"\033[92m[+]    Login: /login  |  User: {ADMIN_USER}\033[0m")
    print(f"\033[96m[+]    🎮 Global Match Mode: {current_mode}\033[0m")
    print(f"\033[96m[+]    🎚️ Risk Mode: {current_risk}\033[0m")
    print(f"\033[96m[+]    ⚡ Speed: {current_speed}x\033[0m")
    return runner


if __name__ == "__main__":
    asyncio.run(start_web_dashboard())