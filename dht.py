# dht.py
import json
import os
import random
import socket
import traceback
import time

from collections import deque
from random import randint
from socket import inet_ntoa
from struct import unpack
from threading import Thread, Timer
from time import sleep

import bencodepy
from bencodepy.exceptions import DecodingError


def bencode(x):
    return bencodepy.encode(x)


def bdecode(x):
    return bencodepy.decode(x)


def to_str(x):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            if isinstance(k, (bytes, bytearray)):
                k = k.decode("utf-8", errors="ignore")
            out[k] = to_str(v)
        return out
    if isinstance(x, list):
        return [to_str(i) for i in x]
    return x


def entropy(length):
    return bytes(randint(0, 255) for _ in range(length))


def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n
    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack("!H", nodes[i + 24:i + 26])[0]
        n.append((nid, ip, port))
    return n


def timer(t, f):
    Timer(t, f).start()


def get_neighbor(target, nid, end=10):
    return target[:end] + nid[end:]


class KNode:
    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTBootstrapStore:
    def __init__(self, path="state/dht_bootstrap.jsonl", ttl_seconds=72 * 3600, max_peers=5000):
        self.path = path
        self.ttl_seconds = float(ttl_seconds)
        self.max_peers = int(max_peers)

        self._peers = {}  # (ip, port) -> last_ok
        self._load_previous()
        self._reset_file_for_new_run()

    def _load_previous(self):
        now = time.time()
        try:
            with open(self.path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue

                    ip = obj.get("ip")
                    port = obj.get("port")
                    last_ok = obj.get("last_ok")

                    if not isinstance(ip, str):
                        continue
                    if not isinstance(port, int):
                        continue
                    if port < 1 or port > 65535:
                        continue
                    if not isinstance(last_ok, (int, float)):
                        continue

                    ts = float(last_ok)
                    if (now - ts) > self.ttl_seconds:
                        continue

                    key = (ip, port)
                    prev = self._peers.get(key)
                    if prev is None or ts > prev:
                        self._peers[key] = ts

        except FileNotFoundError:
            pass
        except Exception:
            pass

        if len(self._peers) > self.max_peers:
            items = sorted(self._peers.items(), key=lambda kv: kv[1], reverse=True)[: self.max_peers]
            self._peers = dict(items)

    def _reset_file_for_new_run(self):
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        try:
            with open(self.path, "w", encoding="utf-8"):
                pass
        except Exception:
            pass

    def mark_ok(self, address):
        ip, port = address
        if not isinstance(ip, str):
            return
        if not isinstance(port, int):
            return
        if port < 1 or port > 65535:
            return

        now = time.time()
        key = (ip, port)

        prev = self._peers.get(key)
        if prev is None or now > prev:
            self._peers[key] = now

        if len(self._peers) > self.max_peers:
            items = sorted(self._peers.items(), key=lambda kv: kv[1], reverse=True)[: self.max_peers]
            self._peers = dict(items)

        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"ip": ip, "port": port, "last_ok": int(now)}, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def sample_previous(self, k):
        if not self._peers:
            return []
        items = list(self._peers.items())
        items.sort(key=lambda kv: kv[1], reverse=True)
        pool = [addr for addr, _ts in items[: max(int(k) * 4, int(k))]]
        n = min(int(k), len(pool))
        return random.sample(pool, n) if len(pool) > n else pool


class DHTProcess(Thread):
    def __init__(
        self,
        master,
        logger,
        random_id_fn,
        ip,
        port,
        bootstrap_nodes,
        max_node_qsize=200,
        rejoin_interval=3,
        token_length=2,
        tid_length=2,
        bootstrap_store_path="state/dht_bootstrap.jsonl",
        bootstrap_ttl_seconds=72 * 3600,
        bootstrap_max_peers=5000,
        bootstrap_extra_k=50,
    ):
        super().__init__(daemon=True)

        self.master = master
        self.logger = logger
        self.random_id_fn = random_id_fn

        self.ip = ip
        self.port = port

        self.bootstrap_nodes = bootstrap_nodes
        self.max_node_qsize = max_node_qsize
        self.rejoin_interval = rejoin_interval
        self.token_length = token_length
        self.tid_length = tid_length

        self.nid = random_id_fn()
        self.nodes = deque(maxlen=max_node_qsize)

        self.rx = 0
        self.tx = 0
        self.q_announce = 0
        self.q_getpeers = 0

        self.process_request_actions = {
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }

        self.status_interval = 0.02
        self.last_status = 0.0
        self.spin_i = 0

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.ip, self.port))
        self.ufd.settimeout(0.1)

        self.bootstrap_store = DHTBootstrapStore(
            path=bootstrap_store_path,
            ttl_seconds=bootstrap_ttl_seconds,
            max_peers=bootstrap_max_peers,
        )
        self.bootstrap_extra = list(self.bootstrap_store.sample_previous(bootstrap_extra_k))

        timer(self.rejoin_interval, self.re_join_dht)

    def send_krpc(self, msg, address):
        try:
            payload = bencode(msg)
            self.ufd.sendto(payload, address)
            self.tx += 1
        except Exception:
            traceback.print_exc()

    def send_find_node(self, address, nid=None):
        nid2 = get_neighbor(nid, self.nid) if nid else self.nid
        tid = entropy(self.tid_length)
        msg = {
            b"t": tid,
            b"y": b"q",
            b"q": b"find_node",
            b"a": {b"id": nid2, b"target": self.random_id_fn()},
        }
        self.send_krpc(msg, address)

    def join_dht(self):
        for address in self.bootstrap_nodes:
            self.send_find_node(address)
        for address in self.bootstrap_extra:
            self.send_find_node(address)

    def re_join_dht(self):
        self.logger.status(
            "rx", self.rx,
            "tx", self.tx,
            "nodes", len(self.nodes),
            "announce", self.q_announce,
            "get_peers", self.q_getpeers,
        )
        if len(self.nodes) == 0:
            self.join_dht()
        timer(self.rejoin_interval, self.re_join_dht)

    def run(self):
        self.re_join_dht()
        while True:
            try:
                try:
                    data, address = self.ufd.recvfrom(65536)
                    self.rx += 1
                except socket.timeout:
                    data = b""
                    address = ("-", 0)

                if not data or data[:1] not in b"dli0123456789":
                    continue

                try:
                    msg = bdecode(data)
                except DecodingError:
                    continue

                msg = to_str(msg)

                y = msg.get("y")
                if isinstance(y, (bytes, bytearray)):
                    msg["y"] = y.decode("ascii", errors="ignore")

                q = msg.get("q")
                if isinstance(q, (bytes, bytearray)):
                    msg["q"] = q.decode("ascii", errors="ignore")

                if msg.get("y") == "q":
                    if msg.get("q") == "announce_peer":
                        self.q_announce += 1
                    elif msg.get("q") == "get_peers":
                        self.q_getpeers += 1

                now = time.time()
                if now - self.last_status >= self.status_interval:
                    spin = "|/-\\"[self.spin_i & 3]
                    self.spin_i += 1

                    self.logger.status(
                        f"{spin}",
                        "rx", self.rx,
                        "tx", self.tx,
                        "nodes", len(self.nodes),
                        "announce", self.q_announce,
                        "get_peers", self.q_getpeers,
                        "last", address[0],
                    )
                    self.last_status = now

                self.on_message(msg, address)

            except Exception:
                traceback.print_exc()

    def process_find_node_response(self, msg, address):
        r = msg.get("r")
        if not isinstance(r, dict):
            return

        raw = r.get("nodes", b"")
        if isinstance(raw, str):
            raw = raw.encode("latin1", errors="ignore")

        nodes = decode_nodes(raw if isinstance(raw, (bytes, bytearray)) else b"")
        if nodes:
            self.bootstrap_store.mark_ok((address[0], address[1]))

        for nid, ip, port in nodes:
            if len(nid) != 20:
                continue
            if ip == self.ip:
                continue
            if port < 1 or port > 65535:
                continue
            self.nodes.append(KNode(nid, ip, port))

    def on_get_peers_request(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            tid = msg["t"]
            token = infohash[: self.token_length]
            reply = {
                "t": tid,
                "y": "r",
                "r": {"id": get_neighbor(infohash, self.nid), "nodes": "", "token": token},
            }
            self.send_krpc(reply, address)
        except KeyError:
            pass

    def on_announce_peer_request(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            token = msg["a"]["token"]

            if infohash[: self.token_length] != token:
                return

            if "implied_port" in msg["a"] and msg["a"]["implied_port"] != 0:
                port = address[1]
            else:
                port = msg["a"]["port"]
                if port < 1 or port > 65535:
                    return

            candidates = [port]
            if address[1] != port:
                candidates.append(address[1])

            for p in candidates:
                self.master.log_infohash(infohash, (address[0], p))

        except Exception:
            pass
        finally:
            self.ok(msg, address)

    def ok(self, msg, address):
        try:
            tid = msg["t"]
            nid = msg["a"]["id"]
            reply = {"t": tid, "y": "r", "r": {"id": get_neighbor(nid, self.nid)}}
            self.send_krpc(reply, address)
        except KeyError:
            pass

    def on_message(self, msg, address):
        try:
            if msg["y"] == "r":
                if "nodes" in msg.get("r", {}):
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q":
                handler = self.process_request_actions.get(msg.get("q"))
                if handler is None:
                    self.play_dead(msg, address)
                else:
                    handler(msg, address)
        except KeyError:
            pass

    def play_dead(self, msg, address):
        try:
            tid = msg["t"]
            reply = {"t": tid, "y": "e", "e": [202, "Server Error"]}
            self.send_krpc(reply, address)
        except KeyError:
            pass

    def auto_send_find_node(self):
        wait = 1.0 / self.max_node_qsize
        while True:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
            except IndexError:
                pass
            sleep(wait)
