#!/usr/bin/env python3
import argparse
import hashlib
from pathlib import Path
from typing import Tuple, Any


def _bdecode(data: bytes, i: int = 0) -> Tuple[Any, int]:
    c = data[i:i+1]
    if c == b"i":
        j = data.index(b"e", i)
        return int(data[i+1:j]), j + 1
    if c == b"l":
        i += 1
        out = []
        while data[i:i+1] != b"e":
            v, i = _bdecode(data, i)
            out.append(v)
        return out, i + 1
    if c == b"d":
        i += 1
        out = {}
        while data[i:i+1] != b"e":
            k, i = _bdecode(data, i)
            if not isinstance(k, (bytes, bytearray)):
                raise ValueError("invalid dict key type in bencode")
            v, i = _bdecode(data, i)
            out[k] = v
        return out, i + 1
    if b"0" <= c <= b"9":
        j = data.index(b":", i)
        n = int(data[i:j])
        start = j + 1
        end = start + n
        return data[start:end], end
    raise ValueError(f"invalid bencode at offset {i}")


def _bencode(x: Any) -> bytes:
    if isinstance(x, int):
        return b"i" + str(x).encode("ascii") + b"e"
    if isinstance(x, (bytes, bytearray)):
        b = bytes(x)
        return str(len(b)).encode("ascii") + b":" + b
    if isinstance(x, str):
        b = x.encode("utf-8")
        return str(len(b)).encode("ascii") + b":" + b
    if isinstance(x, list):
        return b"l" + b"".join(_bencode(i) for i in x) + b"e"
    if isinstance(x, dict):
        # keys must be bytes/str and are sorted lexicographically by raw bytes
        items = []
        for k, v in x.items():
            kb = k if isinstance(k, (bytes, bytearray)) else str(k).encode("utf-8")
            items.append((kb, v))
        items.sort(key=lambda kv: kv[0])
        return b"d" + b"".join(_bencode(k) + _bencode(v) for k, v in items) + b"e"
    raise TypeError(f"unsupported type for bencode: {type(x)!r}")


def torrent_to_magnet(path: Path) -> str:
    raw = path.read_bytes()
    meta, end = _bdecode(raw, 0)
    if end != len(raw):
        raise ValueError("extra data after torrent bencode")
    if not isinstance(meta, dict) or b"info" not in meta:
        raise ValueError("missing info dict")
    info = meta[b"info"]
    info_b = _bencode(info)
    infohash = hashlib.sha1(info_b).hexdigest()

    name = info.get(b"name")
    dn = ""
    if isinstance(name, (bytes, bytearray)):
        try:
            dn = name.decode("utf-8", "strict")
        except UnicodeDecodeError:
            dn = ""

    magnet = f"magnet:?xt=urn:btih:{infohash}"
    if dn:
        from urllib.parse import quote
        magnet += f"&dn={quote(dn)}"
    return magnet


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate a magnet link from a .torrent file (v1 SHA1 infohash).")
    ap.add_argument("torrent", type=Path, help="path to .torrent")
    args = ap.parse_args()
    print(torrent_to_magnet(args.torrent))


if __name__ == "__main__":
    main()
