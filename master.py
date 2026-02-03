# master.py
import json
import os
import random
import threading
import time
from queue import Queue

from bt_metadata import download_metadata


class MetadataPeerStore:
    def __init__(self, path, ttl_seconds=72 * 3600, max_peers=1000):
        self.path = path
        self.ttl_seconds = float(ttl_seconds)
        self.max_peers = int(max_peers)

        self._lock = threading.Lock()
        self._peers = []

        self._load_from_previous_run()
        self._reset_file_for_new_run()

    def _load_from_previous_run(self):
        now = time.time()
        items = {}

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
                    if (now - float(last_ok)) > self.ttl_seconds:
                        continue

                    key = (ip, port)
                    prev = items.get(key)
                    if prev is None or float(last_ok) > prev:
                        items[key] = float(last_ok)

        except FileNotFoundError:
            pass
        except Exception:
            pass

        peers = [(ip, port, ts) for (ip, port), ts in items.items()]
        peers.sort(key=lambda x: x[2], reverse=True)
        self._peers = peers[: self.max_peers]

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

        with self._lock:
            found = False
            for i, (p_ip, p_port, _ts) in enumerate(self._peers):
                if (p_ip, p_port) == key:
                    self._peers[i] = (ip, port, now)
                    found = True
                    break
            if not found:
                self._peers.append((ip, port, now))

            self._peers.sort(key=lambda x: x[2], reverse=True)
            if len(self._peers) > self.max_peers:
                self._peers = self._peers[: self.max_peers]

            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(
                        json.dumps(
                            {"ip": ip, "port": port, "last_ok": int(now)},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
            except Exception:
                pass

    def sample(self, k):
        with self._lock:
            if not self._peers:
                return []
            n = min(int(k), len(self._peers))
            pool = self._peers[:]
        picks = random.sample(pool, n) if len(pool) > n else pool
        return [(ip, port) for ip, port, _ts in picks]


class Master(threading.Thread):
    def __init__(self, max_workers, logger, random_id_fn, storage_info_fn):
        super().__init__(daemon=True)
        self.queue = Queue()
        self.seen = set()

        self.sem = threading.BoundedSemaphore(max_workers)
        self.logger = logger
        self.random_id_fn = random_id_fn
        self.storage_info_fn = storage_info_fn

        self.bad_lock = threading.Lock()
        self.bad = {}
        self.bad_ttl = 300.0

        self.peer_store = MetadataPeerStore(
            path="state/metadata_peers.jsonl",
            ttl_seconds=72 * 3600,
            max_peers=1000,
        )

        self.hint_k = 5

        self.fail_lock = threading.Lock()
        self.fail_counts = {}
        self.fail_window = 180.0
        self.fail_threshold = 3

        self.ok_count = 0
        self.fail_count = 0

        self._hb_last = 0.0
        self._hb_interval = 5.0

    def _mark_bad(self, address):
        now = time.time()
        with self.bad_lock:
            self.bad[address] = now + self.bad_ttl

    def _is_bad(self, address):
        now = time.time()
        with self.bad_lock:
            until = self.bad.get(address)
            if not until:
                return False
            if until <= now:
                try:
                    del self.bad[address]
                except KeyError:
                    pass
                return False
            return True

    def _record_failure(self, address):
        now = time.time()
        with self.fail_lock:
            count, until = self.fail_counts.get(address, (0, now + self.fail_window))
            if until <= now:
                count = 0
                until = now + self.fail_window
            count += 1
            self.fail_counts[address] = (count, until)
            if count >= self.fail_threshold:
                self._mark_bad(address)

    def _heartbeat_maybe(self):
        now = time.time()
        if (now - self._hb_last) < self._hb_interval:
            return
        self._hb_last = now

        try:
            qsize = self.queue.qsize()
        except Exception:
            qsize = -1

        try:
            active = threading.active_count()
        except Exception:
            active = -1

        with self.bad_lock:
            bad_n = len(self.bad)

        self.logger.line(
            "[INFO]",
            "hb",
            "q", qsize,
            "threads", active,
            "bad", bad_n,
            "seen", len(self.seen),
            "ok", self.ok_count,
            "fail", self.fail_count,
        )

    def run(self):
        while True:
            self._heartbeat_maybe()
            address, infohash = self.queue.get()
            self.sem.acquire()
            t = threading.Thread(
                target=self._worker,
                args=(address, infohash),
                daemon=True,
            )
            t.start()

    def _worker(self, address, infohash):
        try:
            result = download_metadata(
                address=address,
                infohash=infohash,
                logger=self.logger,
                random_id_fn=self.random_id_fn,
                storage_info_fn=self.storage_info_fn,
            )
            if result == "ok":
                self.ok_count += 1
                self.peer_store.mark_ok(address)
            else:
                self.fail_count += 1
                if result in {"timeout", "os_error"}:
                    self._record_failure(address)
        finally:
            try:
                self.sem.release()
            except Exception:
                pass

    def _enqueue_once(self, infohash, address):
        if not address:
            return False
        if not isinstance(infohash, (bytes, bytearray)) or len(infohash) != 20:
            return False
        if self._is_bad(address):
            return False

        hid = infohash.hex().upper()
        key = (hid, address[0], address[1])
        if key in self.seen:
            return False

        self.seen.add(key)
        if len(self.seen) > 60000:
            self.seen.clear()

        self.logger.line("[INFO]", "[infohash]", hid, "from", address)
        self.queue.put((address, infohash))
        return True

    def log_infohash(self, infohash, address):
        ok = self._enqueue_once(infohash, address)

        if ok and isinstance(infohash, (bytes, bytearray)) and len(infohash) == 20:
            for peer in self.peer_store.sample(self.hint_k):
                if peer == address:
                    continue
                self._enqueue_once(infohash, peer)
