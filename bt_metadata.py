# bt_metadata.py
import gc
import math
import socket
import time
from hashlib import sha1
from struct import unpack
from time import sleep

import bencodepy

from utils import (
    recv_exact,
    decode_torrent_text,
    pick_torrent_field,
    decode_torrent_path_list,
)


BT_PROTOCOL = b"BitTorrent protocol"
BT_MSG_ID = 20
EXT_HANDSHAKE_ID = 0


def bencode(x):
    return bencodepy.encode(x)


def bdecode(x):
    return bencodepy.decode(x)


def send_message(sock, msg):
    sock.send(unpack(">I", (len(msg)).to_bytes(4, "big"))[0].to_bytes(4, "big") + msg)


def send_message_lenpref(sock, msg):
    sock.send(len(msg).to_bytes(4, "big") + msg)


def send_handshake(sock, infohash, random_id_fn):
    pstrlen = bytes([len(BT_PROTOCOL)])
    reserved = b"\x00\x00\x00\x00\x00\x10\x00\x00"
    peer_id = random_id_fn()
    packet = pstrlen + BT_PROTOCOL + reserved + infohash + peer_id
    sock.send(packet)


def check_handshake(packet, self_infohash):
    if not isinstance(packet, (bytes, bytearray)) or len(packet) < 1:
        return False
    pstrlen = packet[0]
    if pstrlen != len(BT_PROTOCOL):
        return False
    if len(packet) < 1 + pstrlen + 8 + 20:
        return False
    pstr = packet[1 : 1 + pstrlen]
    if pstr != BT_PROTOCOL:
        return False
    infohash = packet[1 + pstrlen + 8 : 1 + pstrlen + 8 + 20]
    return infohash == self_infohash


def send_ext_handshake(sock):
    payload = bencode({b"m": {b"ut_metadata": 1}})
    msg = bytes([BT_MSG_ID]) + bytes([EXT_HANDSHAKE_ID]) + payload
    send_message_lenpref(sock, msg)


def request_metadata(sock, ut_metadata, piece):
    payload = bencode({b"msg_type": 0, b"piece": piece})
    msg = bytes([BT_MSG_ID]) + bytes([ut_metadata]) + payload
    send_message_lenpref(sock, msg)


def recvall(sock, timeout=15):
    sock.setblocking(False)
    total = bytearray()
    begin = time.time()
    while True:
        sleep(0.05)
        now = time.time()
        if total and (now - begin) > timeout:
            break
        if (now - begin) > (timeout * 2):
            break
        try:
            chunk = sock.recv(4096)
            if chunk:
                total.extend(chunk)
                begin = now
        except Exception:
            pass
    return bytes(total)


def parse_ext_handshake(packet):
    if not isinstance(packet, (bytes, bytearray)) or len(packet) < 6:
        return None, None

    try:
        msg_len = unpack(">I", packet[:4])[0]
    except Exception:
        return None, None

    if msg_len <= 2:
        return None, None
    if len(packet) < 4 + msg_len:
        return None, None
    if packet[4] != BT_MSG_ID:
        return None, None
    if packet[5] != EXT_HANDSHAKE_ID:
        return None, None

    payload = packet[6 : 4 + msg_len]
    try:
        d = bdecode(payload)
    except Exception:
        return None, None
    if not isinstance(d, dict):
        return None, None

    m = d.get(b"m")
    ut = None
    size = d.get(b"metadata_size")
    if isinstance(m, dict):
        ut = m.get(b"ut_metadata")
    if isinstance(ut, int) and isinstance(size, int) and size > 0:
        return ut, size
    return None, None


def bencode_next_index(buf, i=0):
    n = len(buf)
    if i >= n:
        return None
    c = buf[i:i + 1]
    if c == b"i":
        j = buf.find(b"e", i + 1)
        return None if j == -1 else j + 1
    if c in (b"l", b"d"):
        i += 1
        while True:
            if i >= n:
                return None
            if buf[i:i + 1] == b"e":
                return i + 1
            ni = bencode_next_index(buf, i)
            if ni is None:
                return None
            i = ni
    if b"0" <= c <= b"9":
        colon = buf.find(b":", i)
        if colon == -1:
            return None
        try:
            ln = int(buf[i:colon])
        except Exception:
            return None
        start = colon + 1
        end = start + ln
        return None if end > n else end
    return None


def split_ut_metadata_message(buf):
    start = buf.find(b"d")
    if start == -1:
        return None, None
    end = bencode_next_index(buf, start)
    if end is None:
        return None, None
    try:
        header = bdecode(buf[start:end])
    except Exception:
        return None, None
    if not isinstance(header, dict):
        return None, None
    return header, buf[end:]


def download_metadata(address, infohash, logger, random_id_fn, storage_info_fn, timeout=6):
    hid = infohash.hex().upper()

    the_socket = None
    try:
        the_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        the_socket.settimeout(timeout)

        logger.meta("connect", hid, address)
        the_socket.connect(address)

        send_handshake(the_socket, infohash, random_id_fn)
        packet = recv_exact(the_socket, 68, timeout)
        if not check_handshake(packet, infohash):
            logger.meta("handshake_fail", hid, address)
            return "handshake_fail"

        send_ext_handshake(the_socket)

        lp = recv_exact(the_socket, 4, timeout)
        if len(lp) != 4:
            logger.meta("ext_fail", hid, address, "reason", "no_len_prefix", "len", len(lp))
            return "ext_fail"

        msg_len = unpack(">I", lp)[0]
        if msg_len <= 0 or msg_len > 2_000_000:
            logger.meta("ext_fail", hid, address, "reason", "bad_len", "msg_len", msg_len)
            return "ext_fail"

        body = recv_exact(the_socket, msg_len, timeout)
        if len(body) != msg_len:
            logger.meta("ext_fail", hid, address, "reason", "short_body", "got", len(body), "want", msg_len)
            return "ext_fail"

        packet = lp + body

        ut_metadata, metadata_size = parse_ext_handshake(packet)
        if ut_metadata is None or metadata_size is None:
            logger.meta("ext_fail", hid, address, "reason", "parse_fail", "len", len(packet))
            return "ext_fail"

        pieces = int(math.ceil(metadata_size / (16.0 * 1024)))
        if pieces <= 0 or pieces > 4096:
            logger.meta("bad_pieces", hid, address, "size", metadata_size, "pieces", pieces)
            return "bad_pieces"

        metadata_parts = []
        for piece in range(pieces):
            request_metadata(the_socket, ut_metadata, piece)
            blob = recvall(the_socket, timeout)
            if not blob:
                continue

            marker = b"ee"
            idx = blob.find(marker)
            if idx != -1:
                metadata_parts.append(blob[idx + len(marker):])
                continue

            header, payload = split_ut_metadata_message(blob)
            if header is None or payload is None:
                continue

            msg_type = header.get(b"msg_type")
            piece_no = header.get(b"piece")
            if msg_type != 1:
                continue
            if piece_no != piece:
                continue
            metadata_parts.append(payload)

        if not metadata_parts:
            logger.meta("no_pieces", hid, address, "size", metadata_size, "pieces", pieces)
            return "no_pieces"

        metadata = b"".join(metadata_parts)

        check_metadata = sha1(metadata).hexdigest().upper()
        if check_metadata != infohash.hex().upper():
            logger.meta("sha1_mismatch", hid, address)
            return "sha1_mismatch"

        meta_data = bdecode(metadata)
        torrent_bytes = b"d4:info" + metadata + b"e"

        info = {"hash_id": infohash.hex().upper()}

        raw_name = pick_torrent_field(meta_data, b"name.utf-8", b"name")
        name = decode_torrent_text(raw_name, meta_data).strip()
        info["hash_name"] = name

        total_size = 0
        files = []
        if isinstance(meta_data, dict):
            if b"files" in meta_data and isinstance(meta_data[b"files"], list):
                for item in meta_data[b"files"]:
                    if not isinstance(item, dict):
                        continue
                    ln = item.get(b"length", 0)
                    if isinstance(ln, int):
                        total_size += ln

                    raw_path = pick_torrent_field(item, b"path.utf-8", b"path")
                    parts = decode_torrent_path_list(raw_path, meta_data)
                    files.append({b"length": ln, b"path": parts})
            else:
                ln = meta_data.get(b"length")
                if isinstance(ln, int):
                    total_size = ln

        info["hash_size"] = str(total_size)
        info["files"] = files
        info["a_ip"] = address[0]

        storage_info_fn(info, torrent_bytes, address)
        del info
        gc.collect()

        logger.meta("saved", hid, address)
        return "ok"

    except socket.timeout:
        logger.meta("timeout", hid, address)
        return "timeout"
    except OSError as e:
        logger.meta("os_error", hid, address, repr(e))
        return "os_error"
    except Exception as e:
        logger.meta("exception", hid, address, repr(e))
        return "exception"
    finally:
        if the_socket is not None:
            try:
                the_socket.close()
            except Exception:
                pass
