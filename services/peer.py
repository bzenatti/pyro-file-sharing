from Pyro5.api import expose, Proxy
import os
import re
import threading
import random
import time
import Pyro5  

from common.config import *
from common.utils import *
from services.tracker import Tracker

GLOBAL_EPOCH = 0
GLOBAL_EPOCH_LOCK = threading.Lock()

@expose
class Peer:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.files = self._load_local_files()
        self.tracker_uri = None
        self.tracker = None
        self.voted_epochs = set()
        self.is_tracker = False
        self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
        self.tracker_timer_lock = threading.Lock()
        self.heartbeat_lock = threading.Lock()
        self.ns = None

    def start(self):
        self.daemon, self.ns = get_daemon_and_ns()
        uri = self.daemon.register(self)
        self.ns.register(self.peer_id, uri)

        print(f"[{self.peer_id}] Registered in Name Server with URI: {uri}")

        tracker_uri = self._lookup_tracker()
        if tracker_uri:
            self.tracker_uri = tracker_uri
            print(f"[{self.peer_id}] Found tracker: {tracker_uri}")
            self.register_files_with_tracker()
        else:
            print(f"[{self.peer_id}] No tracker found.")
            self._start_election_if_needed()

        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        self.daemon.requestLoop()

    def _start_election_if_needed(self):
        tracker_uri = self._lookup_tracker()
        if tracker_uri:
            print(f"[{self.peer_id}] Tracker found, skipping election.")
            self.tracker_uri = tracker_uri
            return

        print(f"[{self.peer_id}] No tracker found, starting election.")
        self._trigger_election()

    def _lookup_tracker(self):
        try:
            _, ns = get_daemon_and_ns()  # Get a new nameserver proxy
            entries = ns.list(prefix=TRACKER_NAME_PREFIX)
            ns._pyroRelease()  # Release the proxy
            if not entries:
                return None
            latest_epoch = max(int(name.split("_")[-1]) for name in entries)
            return entries[f"{TRACKER_NAME_PREFIX}{latest_epoch}"]
        except Exception as e:
            print(f"[{self.peer_id}] Error during tracker lookup: {e}")
            return None

    def receive_vote_request(self, epoch, candidate_id):
        global GLOBAL_EPOCH

        if epoch < GLOBAL_EPOCH:
            print(f"[{self.peer_id}] Rejecting vote for epoch {epoch} (lower than current epoch {GLOBAL_EPOCH})")
            return False

        if epoch in self.voted_epochs:
            print(f"[{self.peer_id}] Already voted in epoch {epoch}")
            return False

        self.voted_epochs.add(epoch)
        print(f"[{self.peer_id}] Voting for {candidate_id} in epoch {epoch}")
        return True

    def _trigger_election(self):
        global GLOBAL_EPOCH, GLOBAL_EPOCH_LOCK

        _, ns = get_daemon_and_ns()

        # Count the number of registered peers BEFORE incrementing epoch
        registered_peers = 0
        for name, uri in ns.list().items():
            if name.startswith("Peer"):
                registered_peers += 1

        # Only increment epoch if there's no tracker and we need to elect one
        with GLOBAL_EPOCH_LOCK:
            GLOBAL_EPOCH += 1
            epoch = GLOBAL_EPOCH

        votes = 1
        peer_proxies = []

        # Iterate through peers again to get proxies
        for name, uri in ns.list().items():
            if name.startswith("Peer") and name != self.peer_id:
                try:
                    proxy = Proxy(uri)
                    peer_proxies.append(proxy)
                except:
                    continue

        for peer in peer_proxies:
            try:
                if peer.receive_vote_request(epoch, self.peer_id):
                    votes += 1
                peer._pyroRelease()
            except:
                continue
        ns._pyroRelease()

        if registered_peers == 1:
            print(f"[{self.peer_id}] Only one peer registered, automatically becoming tracker.")
            self.become_tracker()
        elif votes > len(PEER_NAMES) // 2 or (votes == len(PEER_NAMES) // 2 + 1 and self.peer_id == min(PEER_NAMES)):
            print(f"[{self.peer_id}] Won election with {votes} votes")
            self.become_tracker()
        else:
            print(f"[{self.peer_id}] Lost election with {votes} votes")

    def become_tracker(self):
        self.is_tracker = True
        tracker_instance = Tracker()
        uri = self.daemon.register(tracker_instance)

        global GLOBAL_EPOCH
        _, ns = get_daemon_and_ns()
        tracker_name = f"{TRACKER_NAME_PREFIX}{GLOBAL_EPOCH}"
        ns.register(tracker_name, uri)
        ns._pyroRelease()

        self.tracker_uri = uri
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
                    self._start_election_if_needed()
                    self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)

            try:
                if self.tracker_uri:
                    tracker = Proxy(self.tracker_uri)
                    with self.heartbeat_lock:
                        if tracker.heartbeat():
                            with self.tracker_timer_lock:
                                self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
                    tracker._pyroRelease()
            except Exception as e:
                pass

            time.sleep(HEARTBEAT_INTERVAL)

    def send_heartbeat(self):
        while True:
            if self.is_tracker:
                try:
                    if self.tracker_uri:
                        tracker = Proxy(self.tracker_uri)
                        with self.heartbeat_lock:
                            tracker.ping()
                        tracker._pyroRelease()
                except Exception as e:
                    print(f"[{self.peer_id}] Tracker heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def register_files_with_tracker(self):
        try:
            if self.tracker_uri:
                tracker = Proxy(self.tracker_uri)
                tracker.register_peer(self.peer_id, self.files)
                print(f"[{self.peer_id}] Files registered with tracker.")
                tracker._pyroRelease()
        except Exception as e:
            print(f"[{self.peer_id}] Failed to register files with tracker: {e}")

    def query_and_download(self, file_name):
        try:
            if not self.tracker_uri:
                print(f"[{self.peer_id}] Tracker not available.  Please try again later.")
                return

            tracker = Proxy(self.tracker_uri)
            try:
                owners = tracker.query_file(file_name, _pyroTimeout=5)  # Add timeout
            except Pyro5.errors.CommunicationError as e:
                print(f"[{self.peer_id}] Communication error with tracker: {e}")
                tracker._pyroRelease()
                return
            except Exception as e:
                print(f"[{self.peer_id}] Unexpected error during query: {e}")
                tracker._pyroRelease()
                return
            
            if not owners:
                tracker.log_file_request(self.peer_id, file_name)
                print(f"[{self.peer_id}] File '{file_name}' not found on network.")
                tracker._pyroRelease()
                return

            peer_to_contact = random.choice(owners)
            tracker._pyroRelease()

            _, ns = get_daemon_and_ns()
            try:
                peer_proxy = Proxy(ns.lookup(peer_to_contact), _pyroTimeout=5)  # Add timeout
            except Pyro5.errors.CommunicationError as e:
                print(f"[{self.peer_id}] Communication error with peer {peer_to_contact}: {e}")
                ns._pyroRelease()
                return
            except Exception as e:
                print(f"[{self.peer_id}] Unexpected error during lookup: {e}")
                ns._pyroRelease()
                return

            ns._pyroRelease()

            print(f"[{self.peer_id}] Downloading '{file_name}' from {peer_to_contact}...")

            try:
                content = peer_proxy.get_file(file_name, _pyroTimeout=10)  # Add timeout
            except Pyro5.errors.CommunicationError as e:
                print(f"[{self.peer_id}] Communication error getting file from peer {peer_to_contact}: {e}")
                peer_proxy._pyroRelease()
                return
            except Exception as e:
                print(f"[{self.peer_id}] Unexpected error during get_file: {e}")
                peer_proxy._pyroRelease()
                return

            peer_proxy._pyroRelease()

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