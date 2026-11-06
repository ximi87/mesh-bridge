"""Settings storage.

Everything a user can change lives in ``<DATA_DIR>/config.json`` and is edited
from the web Settings page. Only three things come from the environment,
because they are needed before the web page exists:

    DATA_DIR   where config.json and messages.db live   (default ./data)
    WEB_HOST   address the web server binds to          (default 0.0.0.0)
    WEB_PORT   port of the web server                   (default 8080)
"""
import copy
import json
import os
import secrets

DATA_DIR = os.path.abspath(os.environ.get("DATA_DIR", "data"))
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
WEB_HOST = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("WEB_PORT", "8080"))

DEFAULTS = {
    "general": {
        "name": "Ura",          # shown in the page header and Telegram status lines
        "sign": "",                     # prefix on every outgoing mesh message, e.g. "Base: "
        "public_dashboard": False,      # True: map + messages readable without login (sending still needs login)
        "map_lat": None,                # map start position; empty = fit to the nodes heard
        "map_lon": None,
        "map_zoom": 10,
    },
    "telegram": {
        "enabled": False,
        "token": "",
        "chat_id": "",                  # chat (private, group or channel) that receives mesh messages
        "allowed_users": [],            # Telegram user ids allowed to send into the mesh
        "forward_public": True,
        "forward_private": True,
    },
    "meshtastic": {
        "enabled": False,
        "connection": "tcp",            # tcp | serial
        "host": "",                     # tcp: radio IP or hostname (port 4403)
        "serial_port": "",              # serial: e.g. /dev/serial/by-id/usb-Silicon_Labs_CP2102...
        "channel": 0,                   # channel index bridged as "public"
    },
    "meshcore": {
        "enabled": False,
        "connection": "serial",         # serial | tcp
        "serial_port": "",
        "baudrate": 115200,
        "host": "",
        "tcp_port": 5000,
        "channel": 0,                   # 0 = Public
    },
    "auth": {
        "password_hash": "",            # set on first run from the web setup page
        "session_secret": "",
    },
}

SECRET_FIELDS = {("telegram", "token")}
MASK = "••••••••"


def _merge(base, over):
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(out.get(k), dict) and isinstance(v, dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def _from_legacy_env():
    """Import settings from the old env-file setup (TG_TOKEN, MT_HOST, ...) once."""
    e = os.environ
    if not e.get("TG_TOKEN"):
        return {}
    allowed = [int(x) for x in e.get("TG_ALLOWED_USERS", "").split(",") if x.strip()]
    cfg = {
        "general": {"sign": e.get("SIGN", "")},
        "telegram": {"enabled": True, "token": e["TG_TOKEN"], "chat_id": e.get("TG_CHAT_ID", ""),
                     "allowed_users": allowed},
    }
    if e.get("MT_HOST"):
        cfg["meshtastic"] = {"enabled": True, "connection": "tcp", "host": e["MT_HOST"],
                             "channel": int(e.get("MT_CHANNEL", "0"))}
    if e.get("MC_PORT"):
        cfg["meshcore"] = {"enabled": True, "connection": "serial", "serial_port": e["MC_PORT"],
                           "channel": int(e.get("MC_CHANNEL", "0"))}
    return cfg


def load():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = _merge(DEFAULTS, json.load(f))
    else:
        cfg = _merge(DEFAULTS, _from_legacy_env())
    if not cfg["auth"]["session_secret"]:
        cfg["auth"]["session_secret"] = secrets.token_hex(32)
        save(cfg)
    return cfg


def save(cfg):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = CONFIG_PATH + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)   # holds the bot token
    with os.fdopen(fd, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, CONFIG_PATH)


def public_view(cfg):
    """Config as sent to the browser: no auth section, secrets masked."""
    out = {k: copy.deepcopy(v) for k, v in cfg.items() if k != "auth"}
    for sec, key in SECRET_FIELDS:
        if out[sec].get(key):
            out[sec][key] = MASK
    return out


def _coerce(default, value, path):
    """Convert a value from the browser to the type of the default."""
    if isinstance(default, bool):
        if not isinstance(value, bool):
            raise ValueError(f"{path}: expected true/false")
        return value
    if isinstance(default, int) or path.endswith(("map_lat", "map_lon")):
        if value in ("", None):
            return None if default is None else default
        try:
            return float(value) if path.endswith(("map_lat", "map_lon")) else int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{path}: expected a number")
    if isinstance(default, list):
        if isinstance(value, str):
            value = [x for x in value.replace(" ", ",").split(",") if x]
        try:
            return [int(x) for x in value]
        except (TypeError, ValueError):
            raise ValueError(f"{path}: expected a list of numeric ids")
    return "" if value is None else str(value).strip()


def apply_update(cfg, incoming):
    """Merge a settings form into cfg. Only known keys are accepted; a masked or
    empty secret keeps the stored value."""
    new = copy.deepcopy(cfg)
    for sec, fields in incoming.items():
        if sec == "auth" or sec not in DEFAULTS or not isinstance(fields, dict):
            continue
        for key, value in fields.items():
            if key not in DEFAULTS[sec]:
                continue
            if (sec, key) in SECRET_FIELDS and value in (MASK, "", None):
                continue
            new[sec][key] = _coerce(DEFAULTS[sec][key], value, f"{sec}.{key}")
    _validate(new)
    return new


def _validate(cfg):
    t, mt, mc = cfg["telegram"], cfg["meshtastic"], cfg["meshcore"]
    if t["enabled"] and not t["token"]:
        raise ValueError("Telegram: a bot token is required")
    if t["chat_id"] and not str(t["chat_id"]).lstrip("-").isdigit():
        raise ValueError("Telegram: chat id must be a number (send /id to the bot to find it)")
    if mt["connection"] not in ("tcp", "serial") or mc["connection"] not in ("tcp", "serial"):
        raise ValueError("connection must be tcp or serial")
    if mt["enabled"] and not (mt["host"] if mt["connection"] == "tcp" else mt["serial_port"]):
        raise ValueError("Meshtastic: enter a host or pick a serial port")
    if mc["enabled"] and not (mc["host"] if mc["connection"] == "tcp" else mc["serial_port"]):
        raise ValueError("MeshCore: enter a host or pick a serial port")
    if (mt["enabled"] and mc["enabled"] and mt["connection"] == mc["connection"] == "serial"
            and mt["serial_port"] == mc["serial_port"]):
        raise ValueError("Meshtastic and MeshCore cannot share one serial port")
    for sec in (mt, mc):
        if not 0 <= sec["channel"] <= 7:
            raise ValueError("channel index must be 0-7")
