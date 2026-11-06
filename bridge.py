#!/usr/bin/env python3
"""Mesh Bridge — Meshtastic + MeshCore <-> Telegram + web dashboard.

Public-channel and private (direct) messages from a Meshtastic radio (TCP or
USB serial) and a MeshCore companion radio (USB serial or TCP) are stored in
SQLite, forwarded to a Telegram chat and shown on a web dashboard (map +
messages). Replies can be sent from Telegram (reply to a forwarded message, or
/mt, /mc, /all) or from the dashboard.

Outgoing messages are tracked for delivery: Meshtastic via routing ACK/NAK
(recipient ACK for private, implicit "relayed" ACK for public), MeshCore via
recipient ACK for private and by overhearing repeaters re-sending public posts.

All settings are made in the web UI (Settings page) and stored in
DATA_DIR/config.json. See README.md.
"""
import asyncio
import html
import json
import logging
import os
import secrets
import sqlite3
import sys
import time

import aiohttp
from aiohttp import WSMsgType, web
from pubsub import pub
from telegram import BotCommand, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

import meshtastic.serial_interface
import meshtastic.tcp_interface
from meshtastic import BROADCAST_ADDR
from meshtastic.protobuf import portnums_pb2
from meshcore import EventType, MeshCore

import auth
import config

log = logging.getLogger("bridge")

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
DB_PATH = os.path.join(config.DATA_DIR, "messages.db")
SETUP_CODE_PATH = os.path.join(config.DATA_DIR, "setup-code.txt")

MT_MAX = 200   # bytes, Meshtastic text payload limit is ~228
MC_MAX = 130   # MeshCore channel text limit (~160 incl. sender name)
LIMITS = {"mt": MT_MAX, "mc": MC_MAX}

NET_NAMES = {"mt": "Meshtastic", "mc": "MeshCore"}
MC_TYPES = {1: "client", 2: "repeater", 3: "room", 4: "sensor"}
TRACK_SECS = 120   # how long to wait for ACKs / repeats of an outgoing message


def clip(text, limit):
    b = text.encode()
    if len(b) <= limit:
        return text
    return b[: limit - 3].decode(errors="ignore") + "..."


def list_serial_ports():
    """USB serial ports, preferring the stable /dev/serial/by-id names."""
    by_id = {}
    d = "/dev/serial/by-id"
    if os.path.isdir(d):
        for name in os.listdir(d):
            by_id[os.path.realpath(os.path.join(d, name))] = os.path.join(d, name)
    out = []
    try:
        from serial.tools import list_ports
        for p in list_ports.comports():
            if p.hwid in (None, "n/a"):
                continue   # legacy on-board ttyS ports
            out.append({"path": by_id.get(p.device, p.device), "device": p.device,
                        "description": p.description or ""})
    except Exception as e:
        log.warning("listing serial ports failed: %s", e)
    return out


class Store:
    """SQLite message history; used only from the asyncio loop thread."""

    COLS = ("id", "ts", "net", "direction", "scope", "peer_id", "peer_name",
            "text", "snr", "hops", "via", "status", "tg_id",
            "delivery", "delivery_info", "route", "tg_status_id")
    EXTRA = {"delivery": "TEXT", "delivery_info": "TEXT", "route": "TEXT", "tg_status_id": "INTEGER"}

    def __init__(self, path):
        self.con = sqlite3.connect(path)
        self.con.execute("""CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY, ts REAL, net TEXT, direction TEXT, scope TEXT,
            peer_id TEXT, peer_name TEXT, text TEXT, snr REAL, hops INTEGER,
            via TEXT, status TEXT, tg_id INTEGER)""")
        have = {r[1] for r in self.con.execute("PRAGMA table_info(messages)")}
        for col, typ in self.EXTRA.items():
            if col not in have:
                self.con.execute(f"ALTER TABLE messages ADD COLUMN {col} {typ}")
        self.con.execute("CREATE INDEX IF NOT EXISTS messages_tg ON messages(tg_id)")
        self.con.execute("CREATE INDEX IF NOT EXISTS messages_tgs ON messages(tg_status_id)")
        # anything still "waiting" from before a restart can no longer be tracked
        self.con.execute("UPDATE messages SET delivery='unknown' WHERE delivery IN ('waiting','sending')")
        self.con.commit()

    def add(self, **m):
        m.setdefault("ts", time.time())
        keys = [k for k in self.COLS if k != "id" and k in m]
        cur = self.con.execute(
            f"INSERT INTO messages ({','.join(keys)}) VALUES ({','.join('?' * len(keys))})",
            [m[k] for k in keys])
        self.con.commit()
        return self.get(cur.lastrowid)

    def update(self, mid, **fields):
        keys = [k for k in fields if k in self.COLS and k != "id"]
        self.con.execute(f"UPDATE messages SET {','.join(k + '=?' for k in keys)} WHERE id=?",
                         [fields[k] for k in keys] + [mid])
        self.con.commit()

    def by_tg_status(self, tg_status_id):
        rows = self.con.execute("SELECT * FROM messages WHERE tg_status_id=? ORDER BY id",
                                (tg_status_id,)).fetchall()
        return [dict(zip(self.COLS, r)) for r in rows]

    def get(self, mid):
        row = self.con.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
        return dict(zip(self.COLS, row)) if row else None

    def by_tg(self, tg_id):
        row = self.con.execute("SELECT * FROM messages WHERE tg_id=?", (tg_id,)).fetchone()
        return dict(zip(self.COLS, row)) if row else None

    def recent(self, limit=300, before=None, public_only=False):
        q, where, args = "SELECT * FROM messages", [], []
        if before:
            where.append("id<?")
            args.append(before)
        if public_only:
            where.append("scope<>'dm'")
        if where:
            q += " WHERE " + " AND ".join(where)
        rows = self.con.execute(q + " ORDER BY id DESC LIMIT ?", args + [limit]).fetchall()
        return [dict(zip(self.COLS, r)) for r in reversed(rows)]


class Bridge:
    def __init__(self):
        self.cfg = config.load()
        self.loop = None
        self.app = None          # telegram Application, None when Telegram is off
        self.mt = None
        self.mc = None
        self.mt_id = None
        self.mt_nodes = {}       # last snapshot, kept while Meshtastic is down
        self.mc_battery = None
        self.store = Store(DB_PATH)
        self.sockets = {}        # websocket -> logged in?
        self.mt_pending = {}     # meshtastic packet id -> (message id, dest, sent time)
        self.mc_pending = []     # [message id, channel text as heard, sent time, paths]
        self.tg_edits = {}       # telegram status message id -> pending edit task
        self.tg_seen = {}        # chat id -> who wrote to the bot recently (for the settings page)
        self.tg_error = None
        self.login_fail = {}     # ip -> (count, first failure time)
        self.setup_code = None
        self.restarting = False

    # shortcuts to the current settings
    @property
    def g(self):
        return self.cfg["general"]

    @property
    def tg(self):
        return self.cfg["telegram"]

    @property
    def mtc(self):
        return self.cfg["meshtastic"]

    @property
    def mcc(self):
        return self.cfg["meshcore"]

    def chat_id(self):
        c = str(self.tg.get("chat_id") or "")
        return int(c) if c.lstrip("-").isdigit() else None

    def mt_target(self):
        return self.mtc["host"] if self.mtc["connection"] == "tcp" else self.mtc["serial_port"]

    def mc_target(self):
        return (f"{self.mcc['host']}:{self.mcc['tcp_port']}" if self.mcc["connection"] == "tcp"
                else self.mcc["serial_port"])

    # ── Telegram out ─────────────────────────────────────────────
    async def tg_send(self, text, **kw):
        """Send to the configured chat; None when Telegram is off or not set up yet."""
        chat = self.chat_id()
        if self.app is None or chat is None:
            return None
        try:
            return await self.app.bot.send_message(chat, text, **kw)
        except Exception as e:
            log.warning("telegram send failed: %s", e)
            return None

    # ── Message pipeline ─────────────────────────────────────────
    async def publish(self, msg, kind="msg"):
        data = json.dumps({"type": kind, "msg": msg})
        for ws, logged_in in list(self.sockets.items()):
            if msg.get("scope") == "dm" and not logged_in:
                continue   # private messages only for logged-in viewers
            try:
                await ws.send_str(data)
            except Exception:
                self.sockets.pop(ws, None)

    async def update_msg(self, mid, **fields):
        self.store.update(mid, **fields)
        msg = self.store.get(mid)
        await self.publish(msg, "update")
        if msg.get("tg_status_id"):
            self.schedule_tg_edit(msg["tg_status_id"])

    def schedule_tg_edit(self, tg_status_id):
        # debounce: several repeats can arrive within a second or two
        if tg_status_id in self.tg_edits or self.app is None:
            return

        async def later():
            await asyncio.sleep(2)
            self.tg_edits.pop(tg_status_id, None)
            msgs = self.store.by_tg_status(tg_status_id)
            if not msgs or self.chat_id() is None:
                return
            try:
                await self.app.bot.edit_message_text(self.tg_status_text(msgs), chat_id=self.chat_id(),
                                                     message_id=tg_status_id)
            except Exception as e:
                if "not modified" not in str(e):
                    log.warning("telegram edit failed: %s", e)

        self.tg_edits[tg_status_id] = asyncio.create_task(later())

    @staticmethod
    def delivery_line(m):
        net = NET_NAMES[m["net"]] + (f" private → {m['peer_name']}" if m["scope"] == "dm" else " public")
        icon = {"delivered": "✅", "relayed": "☑️", "repeated": "✅", "waiting": "⏳",
                "sent": "📤", "failed": "❌", "unknown": "❔"}.get(m.get("delivery") or "", "•")
        return f"{icon} {net}: {m.get('delivery_info') or m.get('delivery') or m.get('status')}"

    def tg_status_text(self, msgs):
        lines = "\n".join(self.delivery_line(m) for m in msgs)
        if msgs[0]["via"] == "web":
            return f"🌐 Sent from web:\n{msgs[0]['text']}\n\n{lines}"
        return lines

    async def incoming(self, net, sender, text, extra="", dest=None, snr=None, hops=None, route=None):
        msg = self.store.add(net=net, direction="in", scope="dm" if dest else "public",
                             peer_id=dest, peer_name=sender, text=text, snr=snr, hops=hops,
                             via="mesh", status="received", route=route)
        await self.publish(msg)
        if not (self.tg["forward_private"] if dest else self.tg["forward_public"]):
            return
        head = f"🔒 <b>[{NET_NAMES[net]} private]</b>" if dest else f"📡 <b>[{NET_NAMES[net]}]</b>"
        sent = await self.tg_send(f"{head} <b>{html.escape(sender)}</b>{html.escape(extra)}\n"
                                  f"{html.escape(text)}", parse_mode="HTML")
        if sent:
            self.store.update(msg["id"], tg_id=sent.message_id)

    async def status(self, text):
        await self.tg_send(f"ℹ️ {text}")

    def peer_name(self, net, dest):
        if not dest:
            return None
        for n in self.nodes():
            if n["net"] == net and n["id"] == dest:
                return n["name"]
        return dest[:8]

    async def send(self, nets, text, dest=None, via="telegram"):
        """Send to one or more networks. Delivery is tracked in the background;
        returns the stored messages (status 'sent' or 'failed: ...')."""
        text = self.g["sign"] + text
        msgs = []
        for net in nets:
            sent_text = clip(text, LIMITS[net])
            msg = self.store.add(net=net, direction="out", scope="dm" if dest else "public",
                                 peer_id=dest, peer_name=self.peer_name(net, dest), text=sent_text,
                                 via=via, status="sending", delivery="waiting",
                                 delivery_info="sending…")
            try:
                await (self.mt_send if net == "mt" else self.mc_send)(msg["id"], sent_text, dest)
                self.store.update(msg["id"], status="sent")
            except Exception as e:
                self.store.update(msg["id"], status=f"failed: {e}", delivery="failed",
                                  delivery_info=f"not sent: {e}")
            msg = self.store.get(msg["id"])
            await self.publish(msg)
            msgs.append(msg)
        if via == "web":
            sent = await self.tg_send(self.tg_status_text(msgs))
            if sent:
                for m in msgs:
                    self.store.update(m["id"], tg_status_id=sent.message_id)
        return msgs

    # ── Meshtastic (threaded lib) ────────────────────────────────
    def mt_on_text(self, packet, interface):
        try:
            if packet.get("fromId") == self.mt_id:
                return
            dm = packet.get("toId") == self.mt_id
            if not dm and (packet.get("to") != 0xFFFFFFFF or packet.get("channel", 0) != self.mtc["channel"]):
                return  # other channels
            text = packet.get("decoded", {}).get("text", "")
            node = interface.nodes.get(packet.get("fromId") or "", {}) if interface.nodes else {}
            name = node.get("user", {}).get("longName") or packet.get("fromId") or "?"
            snr = packet.get("rxSnr")
            hops = None
            if packet.get("hopStart") is not None and packet.get("hopLimit") is not None:
                hops = packet["hopStart"] - packet["hopLimit"]
            extra = f" ({packet.get('fromId')})"
            if snr is not None:
                extra += f" SNR {snr}"
            if hops is not None:
                extra += f" hops {hops}"
            dest = packet.get("fromId") if dm else None
            asyncio.run_coroutine_threadsafe(
                self.incoming("mt", name, text, extra, dest, snr, hops), self.loop)
        except Exception:
            log.exception("mt_on_text")

    def mt_on_routing(self, packet, interface):
        """ACK/NAK for a packet we sent (decoded.requestId = our packet id)."""
        try:
            decoded = packet.get("decoded", {})
            if decoded.get("portnum") != "ROUTING_APP":
                return
            entry = self.mt_pending.get(decoded.get("requestId"))
            if entry is None:
                return
            mid, dest, _ = entry
            reason = decoded.get("routing", {}).get("errorReason", "NONE")
            frm = packet.get("fromId")
            hops = None
            if packet.get("hopStart") is not None and packet.get("hopLimit") is not None:
                hops = packet["hopStart"] - packet["hopLimit"]
            fields = None
            if reason != "NONE":
                cur = (self.store_get_threadsafe(mid) or {}).get("delivery")
                if cur not in ("delivered", "relayed"):
                    fields = {"delivery": "failed",
                              "delivery_info": ("not delivered" if dest else "nobody relayed it")
                              + f" ({reason})"}
                self.mt_pending.pop(decoded.get("requestId"), None)
            elif dest and frm == dest:
                name = self.peer_name("mt", dest)
                fields = {"delivery": "delivered", "hops": hops,
                          "delivery_info": f"delivered to {name}"
                          + (f" · {hops} hop{'s' if hops != 1 else ''}" if hops is not None else "")}
                self.mt_pending.pop(decoded.get("requestId"), None)
            elif frm == self.mt_id:
                # implicit ACK: our radio heard a neighbour rebroadcast the packet
                fields = {"delivery": "relayed",
                          "delivery_info": "relayed by a neighbour, waiting for recipient"
                          if dest else "heard a neighbour relay it"}
                if not dest:
                    self.mt_pending.pop(decoded.get("requestId"), None)
            if fields:
                asyncio.run_coroutine_threadsafe(self.update_msg(mid, **fields), self.loop)
        except Exception:
            log.exception("mt_on_routing")

    def store_get_threadsafe(self, mid):
        return asyncio.run_coroutine_threadsafe(self._get(mid), self.loop).result(5)

    async def _get(self, mid):
        return self.store.get(mid)

    def mt_on_lost(self, interface, topic=pub.AUTO_TOPIC):
        log.warning("meshtastic connection lost")
        self.mt = None

    async def track_timeouts(self):
        while True:
            await asyncio.sleep(15)
            now = time.time()
            for pid, (mid, dest, ts) in list(self.mt_pending.items()):
                if now - ts > TRACK_SECS:
                    self.mt_pending.pop(pid, None)
                    m = self.store.get(mid) or {}
                    if m.get("delivery") == "waiting":
                        await self.update_msg(mid, delivery="failed", delivery_info=
                                              "no ACK from recipient" if dest else "no relay heard")
                    elif m.get("delivery") == "relayed" and dest:
                        await self.update_msg(mid, delivery_info="relayed, but recipient never confirmed")

    def mt_connect(self):
        if self.mtc["connection"] == "serial":
            return meshtastic.serial_interface.SerialInterface(self.mtc["serial_port"])
        return meshtastic.tcp_interface.TCPInterface(self.mtc["host"])

    async def mt_keepalive(self):
        if not self.mtc["enabled"]:
            return
        pub.subscribe(self.mt_on_text, "meshtastic.receive.text")
        pub.subscribe(self.mt_on_lost, "meshtastic.connection.lost")
        pub.subscribe(self.mt_on_routing, "meshtastic.receive")
        down_reported = False
        while True:
            if self.mt is None:
                try:
                    self.mt = await asyncio.to_thread(self.mt_connect)
                    self.mt_id = self.mt.getMyNodeInfo()["user"]["id"]
                    log.info("meshtastic connected as %s", self.mt_id)
                    if down_reported:
                        await self.status("Meshtastic reconnected")
                    down_reported = False
                except Exception as e:
                    log.warning("meshtastic connect failed: %s", e)
                    self.mt = None
                    if not down_reported:
                        await self.status(f"Meshtastic unreachable ({self.mt_target()}), retrying")
                        down_reported = True
            await asyncio.sleep(30)

    async def mt_send(self, mid, text, dest=None):
        if self.mt is None:
            raise RuntimeError("Meshtastic not connected")
        pkt = await asyncio.to_thread(
            self.mt.sendData, text.encode(), dest or BROADCAST_ADDR,
            portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP, wantAck=True,
            channelIndex=0 if dest else self.mtc["channel"])
        self.mt_pending[pkt.id] = (mid, dest, time.time())
        self.store.update(mid, delivery_info="waiting for recipient ACK" if dest else "waiting for a relay")

    # ── MeshCore (asyncio lib) ───────────────────────────────────
    def mc_route(self, path_hex, hash_size=1):
        """Names of the repeaters in a MeshCore path (hex of per-hop key prefixes)."""
        if not path_hex:
            return None
        step = 2 * max(1, hash_size)
        names = []
        for i in range(0, len(path_hex), step):
            h = path_hex[i:i + step]
            c = next((c for k, c in (self.mc.contacts or {}).items() if k.startswith(h)), None)
            names.append(c.get("adv_name") if c else h)
        return " → ".join(names)

    async def mc_on_rx(self, event):
        """Raw packets heard by the radio: catch repeaters re-sending our public posts."""
        p = event.payload
        if p.get("payload_typename") != "GRP_TXT" or not p.get("message") or not self.mc_pending:
            return
        now = time.time()
        self.mc_pending = [e for e in self.mc_pending if now - e[2] < TRACK_SECS]
        for entry in self.mc_pending:
            mid, heard_text, _, paths = entry
            if p["message"] != heard_text:
                continue
            paths[p.get("path", "")] = p.get("path_len", 0)
            hs = p.get("path_hash_size", 1)
            first = sorted({(self.mc_route(k[:2 * hs], hs) or "?") for k in paths if k})
            maxhops = max(paths.values()) if paths else 0
            info = f"repeated by {', '.join(first)} · heard {len(paths)}× · up to {maxhops} hop{'s' if maxhops != 1 else ''}"
            await self.update_msg(mid, delivery="repeated", hops=maxhops, delivery_info=info,
                                  route=self.mc_route(p.get("path", ""), hs))

    async def mc_on_chan(self, event):
        p = event.payload
        if p.get("channel_idx") != self.mcc["channel"]:
            return
        route = self.mc_route(p.get("path"), (p.get("path_hash_mode") or 0) + 1) if p.get("path") else None
        raw = p.get("text", "")
        sender, _, text = raw.partition(": ")   # channel text arrives as "Name: msg"
        if not text:
            sender, text = "?", raw
        snr = p.get("SNR")
        hops = p.get("path_len") if p.get("path_len") not in (None, 255) else None
        extra = ""
        if snr is not None:
            extra += f" SNR {snr}"
        if hops is not None:
            extra += f" hops {hops}"
        if route:
            extra += f" via {route}"
        await self.incoming("mc", sender, text, extra, None, snr, hops, route)

    async def mc_on_dm(self, event):
        p = event.payload
        prefix = p.get("pubkey_prefix", "")
        await self.mc.ensure_contacts(follow=True)
        contact = self.mc.get_contact_by_key_prefix(prefix)
        name = contact.get("adv_name") if contact else None
        dest = contact["public_key"] if contact else prefix
        snr = p.get("SNR")
        hops = p.get("path_len") if p.get("path_len") not in (None, 255) else None
        extra = f" ({prefix[:8]})"
        if snr is not None:
            extra += f" SNR {snr}"
        if hops is not None:
            extra += f" hops {hops}"
        await self.incoming("mc", name or prefix[:8], p.get("text", ""), extra, dest, snr, hops)

    async def mc_connect(self):
        if self.mcc["connection"] == "tcp":
            return await MeshCore.create_tcp(self.mcc["host"], self.mcc["tcp_port"])
        return await MeshCore.create_serial(self.mcc["serial_port"], self.mcc["baudrate"])

    async def mc_keepalive(self):
        if not self.mcc["enabled"]:
            return
        down_reported = False
        tick = 0
        while True:
            if self.mc is None or not self.mc.is_connected:
                try:
                    mc = await self.mc_connect()
                    if mc is None:
                        raise RuntimeError("no response")
                    mc.subscribe(EventType.CHANNEL_MSG_RECV, self.mc_on_chan)
                    mc.subscribe(EventType.CONTACT_MSG_RECV, self.mc_on_dm)
                    mc.subscribe(EventType.RX_LOG_DATA, self.mc_on_rx)
                    # decrypting the raw rx log of our channel lets us see repeats + routes
                    mc.set_decrypt_channel_logs(True)
                    await mc.commands.get_channel(self.mcc["channel"])
                    await mc.ensure_contacts()
                    await mc.start_auto_message_fetching()
                    self.mc = mc
                    log.info("meshcore connected: %s", mc.self_info.get("name"))
                    if down_reported:
                        await self.status("MeshCore reconnected")
                    down_reported = False
                except Exception as e:
                    log.warning("meshcore connect failed: %s", e)
                    self.mc = None
                    if not down_reported:
                        await self.status(f"MeshCore unreachable ({self.mc_target()}), retrying")
                        down_reported = True
            else:
                # refresh contacts (new adverts) and battery for the dashboard
                try:
                    await self.mc.ensure_contacts(follow=True)
                    if tick % 10 == 0:
                        r = await self.mc.commands.get_bat()
                        if r.type != EventType.ERROR:
                            self.mc_battery = r.payload
                except Exception as e:
                    log.warning("meshcore refresh failed: %s", e)
            tick += 1
            await asyncio.sleep(30)

    async def mc_send(self, mid, text, dest=None):
        if self.mc is None or not self.mc.is_connected:
            raise RuntimeError("MeshCore not connected")
        if dest:
            self.store.update(mid, delivery_info="waiting for recipient ACK")
            asyncio.create_task(self.mc_dm_track(mid, dest, text))
            return
        res = await self.mc.commands.send_chan_msg(self.mcc["channel"], text)
        if res.type == EventType.ERROR:
            raise RuntimeError(f"MeshCore error: {res.payload}")
        me = (self.mc.self_info or {}).get("name", "")
        self.mc_pending.append([mid, f"{me}: {text}", time.time(), {}])
        self.store.update(mid, delivery="sent", delivery_info="sent · no repeater heard yet")

    async def mc_dm_track(self, mid, dest, text):
        try:
            res = await self.mc.commands.send_msg_with_retry(dest, text)
        except Exception as e:
            res, err = None, str(e)
        else:
            err = "no ACK from recipient"
        name = self.peer_name("mc", dest)
        if res is None or res.type == EventType.ERROR:
            await self.update_msg(mid, delivery="failed", delivery_info=f"not delivered to {name} ({err})")
            return
        c = self.mc.get_contact_by_key_prefix(dest[:12]) or {}
        plen = c.get("out_path_len", -1)
        if res.payload.get("type") == 1 or plen is None or plen < 0:
            how, hops = "flood route", None
        else:
            how, hops = f"direct · {plen} hop{'s' if plen != 1 else ''}", plen
        route = self.mc_route(c.get("out_path"), (c.get("out_path_hash_mode") or 0) + 1)
        await self.update_msg(mid, delivery="delivered", hops=hops, route=route,
                              delivery_info=f"delivered to {name} ({how})" + (f" via {route}" if route else ""))

    # ── Node list for the dashboard ──────────────────────────────
    def nodes(self):
        out = []
        if self.mt is not None and self.mt.nodes:
            self.mt_nodes = dict(self.mt.nodes)
        for nid, n in list(self.mt_nodes.items()):
            u = n.get("user", {})
            pos = n.get("position", {}) or {}
            dm = n.get("deviceMetrics", {}) or {}
            node_id = u.get("id") or nid
            out.append({
                "net": "mt", "id": node_id, "name": u.get("longName") or node_id,
                "short": u.get("shortName"), "hw": u.get("hwModel"), "role": u.get("role"),
                "lat": pos.get("latitude"), "lon": pos.get("longitude"), "alt": pos.get("altitude"),
                "last_heard": n.get("lastHeard"), "snr": n.get("snr"), "hops": n.get("hopsAway"),
                "battery": dm.get("batteryLevel"), "voltage": dm.get("voltage"),
                "self": node_id == self.mt_id,
            })
        if self.mc is not None:
            me = self.mc.self_info or {}
            if me.get("public_key"):
                out.append({
                    "net": "mc", "id": me["public_key"], "name": me.get("name"), "self": True,
                    "role": "companion", "lat": me.get("adv_lat") or None, "lon": me.get("adv_lon") or None,
                    "last_heard": int(time.time()),
                    "battery": None, "voltage": (self.mc_battery or {}).get("level"),
                })
            for key, c in list((self.mc.contacts or {}).items()):
                hops = c.get("out_path_len")
                out.append({
                    "net": "mc", "id": key, "name": c.get("adv_name") or key[:8],
                    "role": MC_TYPES.get(c.get("type"), "node"),
                    "lat": c.get("adv_lat") or None, "lon": c.get("adv_lon") or None,
                    "last_heard": c.get("last_advert"),
                    "hops": hops if hops is not None and hops >= 0 else None,
                    "self": False,
                })
        return out

    def state(self, logged_in):
        mc_name = (self.mc.self_info or {}).get("name") if self.mc else None
        tg = "off"
        if self.tg["enabled"]:
            tg = "error" if self.tg_error else ("no chat" if self.chat_id() is None else "on")
        return {
            "name": self.g["name"],
            "logged_in": logged_in,
            "map": {"lat": self.g["map_lat"], "lon": self.g["map_lon"], "zoom": self.g["map_zoom"]},
            "mt": {"connected": self.mt is not None, "id": self.mt_id, "enabled": self.mtc["enabled"],
                   "max": MT_MAX},
            "mc": {"connected": bool(self.mc and self.mc.is_connected), "name": mc_name,
                   "enabled": self.mcc["enabled"], "max": MC_MAX},
            "telegram": tg,
            "sign": self.g["sign"],
            "nodes": self.nodes(),
        }

    # ── Web: auth ────────────────────────────────────────────────
    def logged_in(self, request):
        a = self.cfg["auth"]
        return auth.valid_session(request.cookies.get(auth.COOKIE), a["session_secret"], a["password_hash"])

    @web.middleware
    async def guard(self, request, handler):
        path = request.path
        is_api = path.startswith("/api/") or path == "/ws"
        if path.startswith("/static/"):
            return await handler(request)
        if not self.cfg["auth"]["password_hash"]:
            # first run: only the setup page works until an admin password is set
            if path in ("/setup", "/api/setup"):
                return await handler(request)
            if is_api:
                raise web.HTTPForbidden(text="setup required")
            raise web.HTTPFound("setup")
        if path in ("/login", "/api/login"):
            return await handler(request)
        if self.logged_in(request):
            return await handler(request)
        if self.g["public_dashboard"] and request.method == "GET" and path in ("/", "/api/state",
                                                                              "/api/messages", "/ws"):
            return await handler(request)
        if is_api:
            raise web.HTTPUnauthorized(text="login required")
        raise web.HTTPFound("login")

    def set_session(self, resp, request):
        a = self.cfg["auth"]
        resp.set_cookie(auth.COOKIE, auth.make_session(a["session_secret"], a["password_hash"]),
                        max_age=auth.SESSION_DAYS * 86400, httponly=True, samesite="Strict",
                        secure=request.secure)

    async def json_body(self, request):
        # JSON-only bodies keep plain cross-site form posts out (CSRF)
        if request.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType()
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="bad json")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="bad json")
        return body

    async def web_setup(self, request):
        body = await self.json_body(request)
        if self.cfg["auth"]["password_hash"]:
            raise web.HTTPConflict(text="already set up")
        if not secrets.compare_digest(str(body.get("code", "")).strip().upper(), self.setup_code or "-"):
            await asyncio.sleep(1)
            raise web.HTTPForbidden(text="wrong setup code — see the bridge log or data/setup-code.txt")
        pw = body.get("password") or ""
        if len(pw) < 8:
            raise web.HTTPBadRequest(text="password must be at least 8 characters")
        self.cfg["auth"]["password_hash"] = auth.hash_password(pw)
        config.save(self.cfg)
        self.setup_code = None
        try:
            os.remove(SETUP_CODE_PATH)
        except OSError:
            pass
        log.info("admin password set")
        resp = web.json_response({"ok": True})
        self.set_session(resp, request)
        return resp

    async def web_login(self, request):
        body = await self.json_body(request)
        ip = request.remote or "?"
        n, first = self.login_fail.get(ip, (0, time.time()))
        if time.time() - first > 900:
            n, first = 0, time.time()
        if n >= 10:
            raise web.HTTPTooManyRequests(text="too many attempts, try again in 15 minutes")
        if not auth.check_password(str(body.get("password", "")), self.cfg["auth"]["password_hash"]):
            self.login_fail[ip] = (n + 1, first)
            await asyncio.sleep(1)
            raise web.HTTPForbidden(text="wrong password")
        self.login_fail.pop(ip, None)
        resp = web.json_response({"ok": True})
        self.set_session(resp, request)
        return resp

    async def web_logout(self, request):
        resp = web.json_response({"ok": True})
        resp.del_cookie(auth.COOKIE)
        return resp

    async def web_password(self, request):
        body = await self.json_body(request)
        if not auth.check_password(str(body.get("current", "")), self.cfg["auth"]["password_hash"]):
            await asyncio.sleep(1)
            raise web.HTTPForbidden(text="current password is wrong")
        pw = body.get("new") or ""
        if len(pw) < 8:
            raise web.HTTPBadRequest(text="password must be at least 8 characters")
        self.cfg["auth"]["password_hash"] = auth.hash_password(pw)
        config.save(self.cfg)
        resp = web.json_response({"ok": True})
        self.set_session(resp, request)   # other sessions are now invalid
        return resp

    # ── Web: settings ────────────────────────────────────────────
    async def web_config_get(self, request):
        return web.json_response({"config": config.public_view(self.cfg),
                                  "serial_ports": list_serial_ports(),
                                  "data_dir": config.DATA_DIR})

    async def web_config_put(self, request):
        body = await self.json_body(request)
        try:
            new = config.apply_update(self.cfg, body)
        except ValueError as e:
            raise web.HTTPBadRequest(text=str(e))
        config.save(new)
        self.cfg = new
        log.info("settings saved from web, restarting")
        self.schedule_restart()
        return web.json_response({"ok": True, "restarting": True})

    async def web_ports(self, request):
        return web.json_response(list_serial_ports())

    async def web_tg_test(self, request):
        body = await self.json_body(request)
        token = body.get("token") or ""
        if token in ("", config.MASK):
            token = self.tg["token"]
        if not token:
            raise web.HTTPBadRequest(text="enter a bot token first")
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                async with s.get(f"https://api.telegram.org/bot{token}/getMe") as r:
                    data = await r.json()
        except Exception as e:
            raise web.HTTPBadGateway(text=f"cannot reach Telegram: {e}")
        if not data.get("ok"):
            raise web.HTTPBadRequest(text=f"Telegram says: {data.get('description', 'invalid token')}")
        u = data["result"]
        return web.json_response({"username": u.get("username"), "name": u.get("first_name")})

    async def web_tg_seen(self, request):
        return web.json_response({"running": self.app is not None, "error": self.tg_error,
                                  "seen": sorted(self.tg_seen.values(), key=lambda x: -x["ts"])})

    async def web_restart(self, request):
        self.schedule_restart()
        return web.json_response({"ok": True})

    def schedule_restart(self):
        if not self.restarting:
            self.restarting = True
            asyncio.create_task(self.restart())

    async def restart(self):
        """Re-exec the process so new settings take effect (works under systemd, Docker or a shell)."""
        await asyncio.sleep(0.7)   # let the HTTP response go out
        try:
            if self.app is not None:
                await asyncio.wait_for(self.app.updater.stop(), 5)
                await asyncio.wait_for(self.app.stop(), 5)
        except Exception as e:
            log.warning("telegram stop: %s", e)
        try:
            if self.mt is not None:
                await asyncio.wait_for(asyncio.to_thread(self.mt.close), 5)
        except Exception as e:
            log.warning("meshtastic close: %s", e)
        try:
            if self.mc is not None:
                await asyncio.wait_for(self.mc.disconnect(), 5)
        except Exception as e:
            log.warning("meshcore close: %s", e)
        logging.shutdown()
        os.execv(sys.executable, sys.orig_argv)

    # ── Web: dashboard ───────────────────────────────────────────
    def page(self, name):
        async def handler(request):
            resp = web.FileResponse(os.path.join(WEB_DIR, name))
            resp.headers["Cache-Control"] = "no-cache"
            return resp
        return handler

    async def web_state(self, request):
        return web.json_response(self.state(self.logged_in(request)))

    async def web_messages(self, request):
        before = request.query.get("before")
        return web.json_response(self.store.recent(
            300, int(before) if before and before.isdigit() else None,
            public_only=not self.logged_in(request)))

    async def web_send(self, request):
        body = await self.json_body(request)
        net, dest, text = body.get("net"), body.get("dest") or None, (body.get("text") or "").strip()
        nets = {"mt": ["mt"], "mc": ["mc"], "all": ["mt", "mc"]}.get(net)
        if not nets or not text or len(text) > 500 or (dest and net == "all"):
            raise web.HTTPBadRequest(text="bad request")
        msgs = await self.send(nets, text, dest, via="web")
        return web.json_response([{"net": m["net"], "ok": not m["status"].startswith("failed"),
                                   "error": m["status"]} for m in msgs])

    async def web_ws(self, request):
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        self.sockets[ws] = self.logged_in(request)
        try:
            async for m in ws:
                if m.type == WSMsgType.ERROR:
                    break
        finally:
            self.sockets.pop(ws, None)
        return ws

    async def start_web(self):
        app = web.Application(middlewares=[self.guard])
        r = app.router
        r.add_get("/", self.page("index.html"))
        r.add_get("/settings", self.page("settings.html"))
        r.add_get("/login", self.page("login.html"))
        r.add_get("/setup", self.page("login.html"))
        r.add_static("/static/", WEB_DIR)
        r.add_post("/api/setup", self.web_setup)
        r.add_post("/api/login", self.web_login)
        r.add_post("/api/logout", self.web_logout)
        r.add_post("/api/password", self.web_password)
        r.add_get("/api/config", self.web_config_get)
        r.add_put("/api/config", self.web_config_put)
        r.add_get("/api/serial-ports", self.web_ports)
        r.add_post("/api/telegram/test", self.web_tg_test)
        r.add_get("/api/telegram/seen", self.web_tg_seen)
        r.add_post("/api/restart", self.web_restart)
        r.add_get("/api/state", self.web_state)
        r.add_get("/api/messages", self.web_messages)
        r.add_post("/api/send", self.web_send)
        r.add_get("/ws", self.web_ws)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, config.WEB_HOST, config.WEB_PORT).start()
        log.info("web interface on http://%s:%s/", config.WEB_HOST, config.WEB_PORT)

    # ── Telegram in ──────────────────────────────────────────────
    async def send_to(self, nets, text, update, dest=None):
        msgs = await self.send(nets, text, dest, via="telegram")
        body = "\n".join(self.delivery_line(m) for m in msgs)
        sent = await update.message.reply_text(body)
        for m in msgs:
            self.store.update(m["id"], tg_status_id=sent.message_id)
        # a fast ACK may already have landed before the status message existed
        self.schedule_tg_edit(sent.message_id)

    def note_sender(self, update):
        """Remember who talks to the bot so the settings page can offer chat/user ids."""
        u, c = update.effective_user, update.effective_chat
        if c is None:
            return
        self.tg_seen[c.id] = {
            "chat_id": c.id, "chat_type": c.type,
            "chat_title": c.title or c.full_name or "",
            "user_id": u.id if u else None,
            "user_name": (u.full_name + (f" (@{u.username})" if u.username else "")) if u else "",
            "ts": time.time(),
        }
        while len(self.tg_seen) > 20:
            self.tg_seen.pop(min(self.tg_seen, key=lambda k: self.tg_seen[k]["ts"]))

    def allowed(self, update):
        self.note_sender(update)
        u, chat = update.effective_user, self.chat_id()
        if u is None or chat is None or update.effective_chat.id != chat:
            return False
        allowed = self.tg["allowed_users"]
        return not allowed or u.id in allowed   # empty list = everyone in the bridged chat

    async def cmd(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self.allowed(update):
            return
        name = update.message.text.split()[0].lstrip("/").split("@")[0]
        text = " ".join(ctx.args)
        if not text:
            await update.message.reply_text(f"usage: /{name} your message")
            return
        nets = {"mt": ["mt"], "mc": ["mc"], "all": ["mt", "mc"]}[name]
        await self.send_to(nets, text, update)

    async def on_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self.allowed(update):
            return
        r = update.message.reply_to_message
        target = self.store.by_tg(r.message_id) if r else None
        if target is None:
            await update.message.reply_text(
                "Reply to a mesh message, or use /mt, /mc or /all followed by your text.")
            return
        dest = target["peer_id"] if target["scope"] == "dm" else None
        await self.send_to([target["net"]], update.message.text, update, dest)

    async def cmd_status(self, update: Update, ctx):
        if not self.allowed(update):
            return
        mt = (f"connected ({self.mt_id})" if self.mt else "DOWN") if self.mtc["enabled"] else "disabled"
        mc = ("connected (" + str(self.mc.self_info.get("name")) + ")"
              if self.mc and self.mc.is_connected else ("DOWN" if self.mcc["enabled"] else "disabled"))
        await update.message.reply_text(f"{self.g['name']}\nMeshtastic: {mt}\nMeshCore: {mc}")

    async def cmd_id(self, update: Update, ctx):
        # helper for setup: tells you your user id and the chat id
        self.note_sender(update)
        await update.message.reply_text(
            f"user id: {update.effective_user.id}\nchat id: {update.effective_chat.id}\n\n"
            "Open the bridge Settings page → Telegram to pick this chat.")

    async def start_telegram(self):
        if not (self.tg["enabled"] and self.tg["token"]):
            return
        try:
            app = Application.builder().token(self.tg["token"]).build()
            app.add_handler(CommandHandler(["mt", "mc", "all"], self.cmd))
            app.add_handler(CommandHandler("status", self.cmd_status))
            app.add_handler(CommandHandler(["id", "start"], self.cmd_id))
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_text))
            await app.initialize()
            await app.start()
            await app.updater.start_polling(allowed_updates=["message"])
            self.app = app
            try:
                await app.bot.set_my_commands([
                    BotCommand("mt", "send to Meshtastic public channel"),
                    BotCommand("mc", "send to MeshCore public channel"),
                    BotCommand("all", "send to both networks"),
                    BotCommand("status", "radio connection status"),
                    BotCommand("id", "show user and chat id"),
                ])
            except Exception:
                pass
            log.info("telegram bot @%s running", app.bot.username)
        except Exception as e:
            self.tg_error = str(e)
            log.error("telegram start failed: %s", e)

    # ── Startup ──────────────────────────────────────────────────
    def first_run_code(self):
        if self.cfg["auth"]["password_hash"]:
            return
        self.setup_code = secrets.token_hex(3).upper()
        with open(SETUP_CODE_PATH, "w") as f:
            f.write(self.setup_code + "\n")
        log.warning("=" * 60)
        log.warning("FIRST RUN — open http://<this-host>:%s/setup", config.WEB_PORT)
        log.warning("Setup code: %s   (also in %s)", self.setup_code, SETUP_CODE_PATH)
        log.warning("=" * 60)

    async def main(self):
        self.loop = asyncio.get_running_loop()
        self.first_run_code()
        await self.start_web()
        await self.start_telegram()
        asyncio.create_task(self.mt_keepalive())
        asyncio.create_task(self.mc_keepalive())
        asyncio.create_task(self.track_timeouts())
        if not (self.mtc["enabled"] or self.mcc["enabled"]):
            log.info("no radio enabled yet — configure one on the Settings page")
        await self.status(f"{self.g['name']} started")
        await asyncio.Event().wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(Bridge().main())
    except KeyboardInterrupt:
        pass
