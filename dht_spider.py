# dht_spider.py
import os
import signal
import sys
from queue import Queue
from threading import Thread

from hashlib import sha1
from random import randint

from logger import Logger
from master import Master
from dht import DHTProcess
from utils import safe_filename


infos = []

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
)

TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2


def entropy(length):
    return bytes(randint(0, 255) for _ in range(length))


def random_id():
    h = sha1()
    h.update(entropy(20))
    return h.digest()


def help():
    print("./dht_spider.py [option]")
    print("  [-s]: Do not store files. Print only.")
    print("  [-p:filename]: Path for magnets log.")
    print("  [-h]: Help.")
    print("  [-t:thread num]: Max concurrent metadata downloads.")
    print("  [-b:(0|1)]: 0 no torrent files. 1 save torrent files.")


def get_option():
    s = 0
    global options
    global path
    global thread_num
    global save_seed

    print()
    print("DHT Spider — AGPL-3.0+")
    print("(C) 2026 ELDOLEO")
    print()

    if len(sys.argv) > 1 and sys.argv[1] == "-h":
        help()
        sys.exit(0)

    while True:
        s += 1
        try:
            options.append(sys.argv[s])
        except IndexError:
            break

    for i in options:
        if i == "-s":
            path = "-s"
            save_seed = 0
        elif i.startswith("-p:"):
            path = i[3:]
        elif i.startswith("-t:"):
            try:
                thread_num = int(i[3:])
            except ValueError:
                pass
        elif i.startswith("-b:"):
            try:
                save_seed = int(i[3:])
            except ValueError:
                pass

    if path == "":
        path = "hash.log"
    if thread_num == 0:
        thread_num = 100
    if save_seed not in (0, 1):
        save_seed = 1


class Watcher:
    def __init__(self):
        self.child = os.fork()
        if self.child == 0:
            return
        self.watch()

    def watch(self):
        try:
            os.wait()
        except KeyboardInterrupt:
            print("[EXIT] Control-C")
            self.kill()
        sys.exit()

    def kill(self):
        try:
            os.kill(self.child, signal.SIGKILL)
        except OSError:
            pass


def storage_info(info, metadate_old, address, logger):
    global infos
    global save_seed
    global path

    count = 0
    flag = 0
    fo = None

    if path != "-s":
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as test_fo:
                test = test_fo.read()
            if str(info["hash_id"]) in test:
                flag = 1
        except FileNotFoundError:
            open(path, "w", encoding="utf-8").close()

    hash_id = str(info.get("hash_id", ""))
    if (hash_id not in infos) and (flag == 0):
        if path != "-s":
            fo = open(path, "a", encoding="utf-8")
            fo.write(f"BT Name:{info.get('hash_name')}\n")
            fo.write(f"Sender:{address}\n")
            fo.write(f"infohash:{hash_id}\n")
            fo.write(f"magnet:?xt=urn:btih:{hash_id}\n")

            name = info.get("hash_name") or ""
            sender = f"{address[0]}:{address[1]}"
            logger.torrent_block(name=name, sender=sender, infohash_hex=hash_id)

        for f in info.get("files", []):
            if count == 10:
                break

            p = ""
            try:
                p = str(f.get("path", [""])[0])
            except Exception:
                p = ""

            length = f.get("length", "")
            logger.line("   " + str(p) + " " + str(length))
            if fo is not None:
                fo.write(f"   {p} {length}\n")

            count += 1

        if fo is not None:
            fo.write("\n\n")
            fo.close()

            if save_seed == 1:
                os.makedirs("BT", exist_ok=True)

                raw_name = str(info.get("hash_name", "")).strip()
                torrent_name = safe_filename(raw_name, fallback=hash_id or "unknown")

                torrent_path = os.path.join("BT", f"{torrent_name}.torrent")
                with open(torrent_path, "wb") as ftest:
                    ftest.write(metadate_old)

        infos.append(hash_id)


if __name__ == "__main__":
    path = ""
    thread_num = 0
    save_seed = -1
    options = []

    trans_queue = Queue()

    get_option()
    print(path)
    print(thread_num)
    print(save_seed)

    if (path != "-s") and (save_seed == 1):
        if not os.path.exists("BT/"):
            os.makedirs("BT/")

    Watcher()

    logger = Logger(status_interval=0.02, enable_status=True, enable_ansi=True)

    master = Master(
        max_workers=thread_num,
        logger=logger,
        random_id_fn=random_id,
        storage_info_fn=lambda info, torrent_bytes, address: storage_info(info, torrent_bytes, address, logger),
    )
    master.start()

    print("Receiving datagrams on :6882")
    dht = DHTProcess(
        master=master,
        logger=logger,
        random_id_fn=random_id,
        ip="0.0.0.0",
        port=6882,
        bootstrap_nodes=BOOTSTRAP_NODES,
        max_node_qsize=200,
        rejoin_interval=RE_JOIN_DHT_INTERVAL,
        token_length=TOKEN_LENGTH,
        tid_length=TID_LENGTH,
    )
    dht.start()

    Thread(target=dht.auto_send_find_node, daemon=True).start()
    Thread(target=lambda: None, daemon=True).start()
    while True:
        signal.pause()
