from Pyro5.api import expose, Proxy
import os
import re
import threading
import random
import time

from common.config import *
from common.utils import *
from services.tracker import Tracker


@expose
class Peer:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.files = self._load_local_files()
        self.tracker_uri = None
        self.tracker = None
        self.epoch = 0
        self.voted_epochs = set()
        self.is_tracker = False
        self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
        self.tracker_timer_lock = threading.Lock()
        self.heartbeat_lock = threading.Lock()

    def start(self):
        self.daemon, self.ns = get_daemon_and_ns()
        uri = self.daemon.register(self)
        self.ns.register(self.peer_id, uri)

        print(f"[{self.peer_id}] Registered in Name Server with URI: {uri}")

        tracker_uri = self._lookup_tracker()
        if tracker_uri:
            self.tracker_uri = tracker_uri
            self.tracker = Proxy(tracker_uri)
            print(f"[{self.peer_id}] Found tracker: {tracker_uri}")
            self.register_files_with_tracker()
        else:
            print(f"[{self.peer_id}] No tracker found. May trigger election.")

        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()

        self.daemon.requestLoop()

    def _lookup_tracker(self):
        try:
            entries = self.ns.list(prefix=TRACKER_NAME_PREFIX)
            if not entries:
                return None
            latest_epoch = max(int(name.split("_")[-1]) for name in entries)
            return entries[f"{TRACKER_NAME_PREFIX}{latest_epoch}"]
        except Exception as e:
            print(f"[{self.peer_id}] Error during tracker lookup: {e}")
            return None

    def receive_vote_request(self, epoch, candidate_id):
        if epoch in self.voted_epochs:
            print(f"[{self.peer_id}] Already voted in epoch {epoch}")
            return False
        self.voted_epochs.add(epoch)
        print(f"[{self.peer_id}] Voting for {candidate_id} in epoch {epoch}")
        return True

    def _trigger_election(self):
        self.epoch += 1
        votes = 1
        peer_proxies = []

        for name, uri in self.ns.list().items():
            if name.startswith("Peer") and name != self.peer_id:
                try:
                    proxy = Proxy(uri)
                    peer_proxies.append(proxy)
                except:
                    continue

        for peer in peer_proxies:
            try:
                if peer.receive_vote_request(self.epoch, self.peer_id):
                    votes += 1
            except:
                continue

        if votes > len(PEER_NAMES) // 2:
            print(f"[{self.peer_id}] Won election with {votes} votes")
            self.become_tracker()
        else:
            print(f"[{self.peer_id}] Lost election with {votes} votes")

    def become_tracker(self):
        self.is_tracker = True
        tracker_instance = Tracker()
        uri = self.daemon.register(tracker_instance)
        tracker_name = f"{TRACKER_NAME_PREFIX}{self.epoch}"
        self.ns.register(tracker_name, uri)
        self.tracker_uri = uri
        self.tracker = Proxy(uri)
        print(f"[{self.peer_id}] Became tracker: {tracker_name} → {uri}")
        self.register_files_with_tracker()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def monitor_heartbeat(self):
        while True:
            if self.is_tracker:
                time.sleep(1)
                continue

            now = time.time()
            with self.tracker_timer_lock:
                if now > self.timer_expiry:
                    print(f"[{self.peer_id}] Tracker timeout. Starting election.")
                    self._trigger_election()
                    self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)

            try:
                if self.tracker:
                    with self.heartbeat_lock:
                        if self.tracker.heartbeat():
                            with self.tracker_timer_lock:
                                self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
            except:
                pass

            time.sleep(HEARTBEAT_INTERVAL)

    def send_heartbeat(self):
        while True:
            if self.is_tracker:
                try:
                    with self.heartbeat_lock:
                        self.tracker.ping()
                    print(f"[{self.peer_id}] Tracker heartbeat sent.")
                except Exception as e:
                    print(f"[{self.peer_id}] Tracker heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def register_files_with_tracker(self):
        try:
            if self.tracker:
                self.tracker.register_peer(self.peer_id, self.files)
                print(f"[{self.peer_id}] Files registered with tracker.")
        except Exception as e:
            print(f"[{self.peer_id}] Failed to register files with tracker: {e}")

    def query_and_download(self, file_name):
        try:
            owners = self.tracker.query_file(file_name)
            if not owners:
                print(f"[{self.peer_id}] File '{file_name}' not found on network.")
                return

            peer_to_contact = random.choice(owners)

            peer_proxy = Proxy(self.ns.lookup(peer_to_contact))
            print(f"[{self.peer_id}] Downloading '{file_name}' from {peer_to_contact}...")

            content = peer_proxy.get_file(file_name)
            if content is None:
                print(f"[{self.peer_id}] Failed to download: file not available on peer.")
                return

            with open(os.path.join("files", file_name), "w") as f:
                f.write(content)
            self.files.append(file_name)
            print(f"[{self.peer_id}] File '{file_name}' downloaded successfully.")

            self.register_files_with_tracker()

        except Exception as e:
            print(f"[{self.peer_id}] Error during file query/download: {e}")

    def get_file(self, file_name):
        file_path = os.path.join("files", file_name)
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return f.read()
        else:
            return None
        
    def _load_local_files(self):
        files_dir = os.path.join(os.getcwd(), "files")
        all_files = os.listdir(files_dir)

        def extract_file_number(filename):
            match = re.search(r"\d+", filename) 
            return int(match.group()) if match else None

        file_numbers = [
            extract_file_number(f) for f in all_files if extract_file_number(f) is not None
        ]

        def is_prime(n): 
            return n in {1,2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47}

        peer_num = int(re.search(r"\d+", self.peer_id).group())

        if peer_num == 1:
            selected = [n for n in file_numbers if n % 2 == 0]
        elif peer_num == 2:
            selected = [n for n in file_numbers if n % 3 == 0]
        elif peer_num == 3:
            selected = [n for n in file_numbers if n % 4 == 0]
        elif peer_num == 4:
            selected = [n for n in file_numbers if n % 5 == 0]
        elif peer_num == 5:
            selected = [n for n in file_numbers if is_prime(n)]
        else:
            selected = []

        return [f"file{n}.txt" for n in selected if os.path.exists(os.path.join(files_dir, f"file{n}.txt"))]