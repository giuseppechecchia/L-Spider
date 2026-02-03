import sys
import bencodepy
from hashlib import sha1

def to_str(x):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            if isinstance(k, (bytes, bytearray)):
                k = k.decode("utf-8", errors="replace")
            out[k] = to_str(v)
        return out
    if isinstance(x, list):
        return [to_str(i) for i in x]
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return repr(x)
    return x

def main(path):
    raw = open(path, "rb").read()
    t = bencodepy.decode(raw)
    info = t.get(b"info") or {}
    info_bencoded = bencodepy.encode(info)
    infohash = sha1(info_bencoded).hexdigest().upper()

    name = info.get(b"name", b"")
    if isinstance(name, (bytes, bytearray)):
        name = name.decode("utf-8", errors="replace")

    total = 0
    files = []
    if b"files" in info:
        for f in info.get(b"files", []):
            ln = int(f.get(b"length", 0))
            total += ln
            path_parts = f.get(b"path", [])
            parts = []
            for p in path_parts:
                if isinstance(p, (bytes, bytearray)):
                    parts.append(p.decode("utf-8", errors="replace"))
                else:
                    parts.append(str(p))
            files.append((" / ".join(parts), ln))
    else:
        total = int(info.get(b"length", 0))

    announce = t.get(b"announce")
    if isinstance(announce, (bytes, bytearray)):
        announce = announce.decode("utf-8", errors="replace")

    print("name:", name)
    print("infohash:", infohash)
    print("total_bytes:", total)
    if announce:
        print("announce:", announce)

    if files:
        print("files:")
        for p, ln in files[:50]:
            print(" ", ln, p)

if __name__ == "__main__":
    main(sys.argv[1])
