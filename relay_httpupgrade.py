# relay_httpupgrade.py
# ══════════════════════════════════════════════════════════════════════════════
# VLESS روی HTTPUpgrade — پیاده‌سازی واقعی (نه دمو)
#
# چرا فایل و پورت جدا (مثل tcp_relay.py)؟
# «HTTPUpgrade» یک هندشیک HTTP/1.1 واقعیه: کلاینت یک GET با هدر
#   Connection: Upgrade
#   Upgrade: websocket
# می‌فرسته (دقیقاً همین‌طور در Xray-core پیاده‌سازی شده — بدون Sec-WebSocket-Key)
# و سرور باید مستقیماً «101 Switching Protocols» برگردونه و از همون لحظه، سوکت رو
# به‌صورت خام (بدون فریم WS) به دو طرف تونل بده.
#
# ASGI/uvicorn (که main.py روش اجرا می‌شه) فقط دو نوع پروتکل رسمی داره: «http»
# و «websocket» (که خودش هندشیک RFC6455 با Sec-WebSocket-Key/Accept رو الزامی
# می‌کنه). یعنی نمی‌شه یک هندشیک HTTPUpgrade واقعی (بدون Sec-WebSocket-Key) رو
# از داخل یک روت FastAPI/uvicorn به‌درستی قبول کرد؛ تنها راه درست و سازگار با
# کلاینت‌های واقعی (Xray-core / v2rayNG / Happ و ...)، باز کردن این هندشیک با
# دست، روی یک TCP listener مستقل (asyncio.start_server) هست — دقیقاً به همون
# دلیلی که VLESS-TCP خام هم به یک پورت جدا نیاز داشت.
#
# نتیجه: این ترنسپورت هم — مثل vless-tcp — روی «پورت HTTP اصلی» سرو نمی‌شه و به
# یک پورت TCP جداگانه نیاز داره (پیش‌فرض 6544، با HTTPUPGRADE_LISTEN_PORT قابل
# تغییر). اگر پشت Railway هستی، دقیقاً مثل VLESS-TCP باید از قابلیت TCP Proxy
# ریلوی استفاده کنی و آدرس/پورت عمومی‌ش رو در تب تنظیمات وارد کنی.

import asyncio
import os
import re
import socket
from datetime import datetime

logger = None  # در start_httpupgrade_relay() از main ست می‌شه

HTTPUPGRADE_LISTEN_PORT = int(os.environ.get("HTTPUPGRADE_LISTEN_PORT", "6544"))
RELAY_BUF = 256 * 1024
HEADER_MAX_BYTES = 8 * 1024
HANDSHAKE_TIMEOUT = 15.0

_server = None

_REQUEST_LINE_RE = re.compile(rb"^([A-Z]+)\s+(\S+)\s+HTTP/1\.[01]$")


async def _read_http_headers(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    """تا رسیدن به \\r\\n\\r\\n می‌خونه؛ اگه چیزی بعد از هدرها هم رسیده باشه
    (pipelined) همون رو هم برمی‌گردونه تا از دست نره."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        if len(buf) > HEADER_MAX_BYTES:
            raise ValueError("header too large")
        chunk = await reader.read(4096)
        if not chunk:
            raise ValueError("connection closed before headers")
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    return head, rest


def _parse_headers(head: bytes) -> tuple[str, str, dict]:
    lines = head.split(b"\r\n")
    if not lines:
        raise ValueError("empty request")
    m = _REQUEST_LINE_RE.match(lines[0])
    if not m:
        raise ValueError("bad request line")
    method = m.group(1).decode()
    path = m.group(2).decode()
    headers = {}
    for line in lines[1:]:
        if b":" not in line:
            continue
        k, _, v = line.partition(b":")
        headers[k.strip().lower().decode()] = v.strip().decode()
    return method, path, headers


def _client_ip_from(headers: dict, writer: asyncio.StreamWriter) -> str:
    fwd = headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    try:
        peer = writer.get_extra_info("peername")
        return peer[0] if peer else "نامشخص"
    except Exception:
        return "نامشخص"


def _extract_uuid(path: str) -> str | None:
    # مسیر مورد انتظار: /httpupgrade/<uuid>  (دقیقاً شبیه /ws/<uuid>)
    parts = [p for p in path.split("?")[0].split("/") if p]
    if len(parts) >= 2 and parts[0] == "httpupgrade":
        return parts[1]
    return None


async def _pipe(reader, writer, conn_id, uid, check_and_use, throttle, prefix_first_downlink: bool):
    """یک جهت از تونل رو پمپ می‌کنه. prefix_first_downlink فقط برای جهت
    target→client لازمه (پیشوند ۲ بایت صفر که پروتکل VLESS برای اولین پاسخ
    انتظار داره)."""
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            if not await check_and_use(uid, len(data)):
                break
            await throttle(uid, len(data))
            payload = (b"\x00\x00" + data) if (prefix_first_downlink and first) else data
            first = False
            writer.write(payload)
            if writer.transport.get_write_buffer_size() > RELAY_BUF:
                await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.write_eof()
        except Exception:
            pass


async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    from main import (
        LINKS, LINKS_LOCK, stats, hourly_traffic, connections, error_logs,
        is_link_allowed, is_ip_allowed, save_state, log_activity, now_ir,
    )
    from relay_vless import parse_vless_header
    from speed_limit import throttle
    import secrets as _secrets

    conn_id = _secrets.token_urlsafe(6)
    target_writer = None
    uid = None
    ip = "نامشخص"

    async def check_and_use(u: str, n: int) -> bool:
        async with LINKS_LOCK:
            link = LINKS.get(u)
            if link is None:
                return False
            if not is_link_allowed(link):
                return False
            link["used_bytes"] += n
            stats["total_bytes"] += n
            hourly_traffic[now_ir().strftime("%H:00")] += n
        return True

    try:
        head, leftover = await asyncio.wait_for(_read_http_headers(reader), timeout=HANDSHAKE_TIMEOUT)
        method, path, headers = _parse_headers(head)
        ip = _client_ip_from(headers, writer)

        conn_upgrade_ok = "upgrade" in headers.get("connection", "").lower()
        upgrade_ok = headers.get("upgrade", "").lower() == "websocket"
        uid = _extract_uuid(path)

        if method != "GET" or not conn_upgrade_ok or not upgrade_ok or not uid:
            writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return

        async with LINKS_LOCK:
            link = LINKS.get(uid)

        if not is_link_allowed(link):
            logger and logger.warning(f"🚫 HTTPUpgrade rejected uuid={uid[:8]}… (not allowed)")
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return

        if not is_ip_allowed(link, uid, ip):
            log_activity("connection", f"اتصال HTTPUpgrade {ip} به کانفیگ «{link.get('label','?')}» رد شد (محدودیت IP)", "warn")
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return

        # هندشیک را قبول کن — از همین لحظه سوکت خام و دوطرفه‌ست (بدون فریم WS)
        writer.write(
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Connection: Upgrade\r\n"
            b"Upgrade: websocket\r\n"
            b"\r\n"
        )
        await writer.drain()

        connections[conn_id] = {
            "uuid": uid, "ip": ip, "transport": "vless-httpupgrade",
            "connected_at": datetime.now().isoformat(), "bytes": 0,
        }
        logger and logger.info(f"✅ HTTPUpgrade [{conn_id}] uuid={uid[:8]}… ip={ip} total={len(connections)}")
        log_activity("connection", f"اتصال HTTPUpgrade جدید از {ip} (کانفیگ {link.get('label','?')})", "info")

        # اولین چانک VLESS ممکنه همراه leftover رسیده باشه، وگرنه از سوکت بخون
        first_chunk = leftover
        if len(first_chunk) < 24:
            more = await asyncio.wait_for(reader.read(RELAY_BUF), timeout=HANDSHAKE_TIMEOUT)
            if not more:
                return
            first_chunk += more

        command, address, port, payload = await parse_vless_header(first_chunk)

        if not await check_and_use(uid, len(first_chunk)):
            return
        stats["total_requests"] += 1
        connections[conn_id]["bytes"] += len(first_chunk)
        logger and logger.info(f"➡️  [{conn_id}] → {address}:{port}")

        target_reader, target_writer = await asyncio.wait_for(
            asyncio.open_connection(address, port), timeout=10.0
        )
        sock = target_writer.transport.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if payload:
            target_writer.write(payload)
            await target_writer.drain()

        done, pending = await asyncio.wait(
            {
                asyncio.create_task(_pipe(reader, target_writer, conn_id, uid, check_and_use, throttle, False)),
                asyncio.create_task(_pipe(target_reader, writer, conn_id, uid, check_and_use, throttle, True)),
            },
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        asyncio.create_task(save_state())

    except asyncio.TimeoutError:
        stats["total_errors"] += 1
        error_logs.append({"error": "httpupgrade handshake timeout", "time": datetime.now().isoformat()})
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now().isoformat()})
        logger and logger.error(f"HTTPUpgrade relay error [{conn_id}]: {exc}")
    finally:
        if target_writer:
            try:
                target_writer.close()
                await target_writer.wait_closed()
            except Exception:
                pass
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        connections.pop(conn_id, None)
        logger and logger.info(f"🔌 HTTPUpgrade closed [{conn_id}]")


async def start_httpupgrade_relay(app_logger=None):
    global _server, logger
    logger = app_logger
    try:
        _server = await asyncio.start_server(_handle_client, "0.0.0.0", HTTPUPGRADE_LISTEN_PORT)
        logger and logger.info(f"VLESS-HTTPUpgrade relay listening on 0.0.0.0:{HTTPUPGRADE_LISTEN_PORT}")
    except Exception as exc:
        logger and logger.warning(f"VLESS-HTTPUpgrade relay could not start on port {HTTPUPGRADE_LISTEN_PORT}: {exc}")


async def stop_httpupgrade_relay():
    global _server
    if _server:
        _server.close()
        try:
            await _server.wait_closed()
        except Exception:
            pass
        _server = None
