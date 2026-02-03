# utils.py
import re
import time
import unicodedata

try:
    from wcwidth import wcwidth
except ImportError:
    def wcwidth(_c):
        return 1


def recv_exact(sock, n, timeout):
    sock.settimeout(timeout)
    buf = bytearray()
    start = time.time()
    while len(buf) < n:
        if time.time() - start > timeout:
            break
        chunk = sock.recv(n - len(buf))
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(s):
    return _ANSI_RE.sub("", s)


def truncate_visual(s, max_width):
    out = []
    w = 0
    i = 0
    n = len(s)
    while i < n:
        if s[i] == "\x1b":
            m = _ANSI_RE.match(s, i)
            if m:
                out.append(m.group(0))
                i = m.end()
                continue
        ch = s[i]
        cw = wcwidth(ch)
        if cw < 0:
            cw = 0
        if w + cw > max_width:
            break
        out.append(ch)
        w += cw
        i += 1
    return "".join(out)


def _torrent_encoding(info_dict):
    if not isinstance(info_dict, dict):
        return None
    enc = info_dict.get(b"encoding")
    if isinstance(enc, (bytes, bytearray)):
        try:
            s = enc.decode("ascii", errors="ignore").strip()
            if s:
                return s
        except Exception:
            return None
    return None


def decode_torrent_text(value, info_dict=None):
    if value is None:
        return ""
    if not isinstance(value, (bytes, bytearray)):
        return str(value)

    try:
        return value.decode("utf-8")
    except Exception:
        pass

    enc = _torrent_encoding(info_dict)
    if enc:
        try:
            return value.decode(enc, errors="replace")
        except Exception:
            pass

    return value.decode("utf-8", errors="replace")


def pick_torrent_field(info_dict, utf8_key, plain_key):
    if not isinstance(info_dict, dict):
        return None
    v = info_dict.get(utf8_key)
    if v is not None:
        return v
    return info_dict.get(plain_key)


def decode_torrent_path_list(path_list, info_dict=None):
    parts = []
    if not isinstance(path_list, list):
        return parts
    for p in path_list:
        parts.append(decode_torrent_text(p, info_dict))
    return parts


def safe_filename(name, fallback="unknown", max_len=180):
    s = name if name else fallback
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("/", "_")
    s = s.replace("\\", "_")
    s = re.sub(r"[\x00-\x1f\x7f]", "_", s)
    s = s.strip().strip(".")
    if not s:
        s = fallback
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s
