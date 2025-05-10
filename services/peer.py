from Pyro5.api import expose, Proxy, locate_ns
import os
import re
import threading
import random
import time

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
        self.tracker_peer_id = None
        self.voted_epochs = set()
        self.is_tracker = False
        self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
        self.tracker_timer_lock = threading.Lock()
        self.active_peers = {}
        self.daemon = None
        self.ns = None

    def start(self):
        self.daemon, self.ns = get_daemon_and_ns()
        uri = self.daemon.register(self)
        self.ns.register(self.peer_id, uri)
        print(f"[{self.peer_id}] Registered in Name Server with URI: {uri}")
        
        threading.Thread(target=self.tracker_discovery_and_election, daemon=True).start()
        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        
        self.daemon.requestLoop()

    def _lookup_tracker(self):
        ns = locate_ns()
        try:
            entries = ns.list(prefix=TRACKER_NAME_PREFIX)
            result = None
            if entries:
                latest_epoch = max(int(name.split("_")[-1]) for name in entries)
                result = entries[f"{TRACKER_NAME_PREFIX}{latest_epoch}"]
            ns._pyroRelease()
            return result
        except Exception as e:
            print(f"[{self.peer_id}] Error during tracker lookup: {e}")
            ns._pyroRelease()
            return None

    def _discover_tracker_peer_id(self):
        for name, uri in self.ns.list(prefix="Peer").items():
            if uri == self.tracker_uri:
                return name
        return None

    def tracker_discovery_and_election(self):
        time.sleep(1)  
        
        ns = locate_ns()
        
        try:
            entries = ns.list(prefix=TRACKER_NAME_PREFIX)
            if entries:
                latest_epoch = max(int(name.split("_")[-1]) for name in entries)
                self.tracker_uri = entries[f"{TRACKER_NAME_PREFIX}{latest_epoch}"]
                
                for name, uri in ns.list(prefix="Peer").items():
                    if uri == self.tracker_uri:
                        self.tracker_peer_id = name
                        break
                        
                print(f"[{self.peer_id}] Found tracker: {self.tracker_uri}")
                self.register_files_with_tracker()
            else:
                print(f"[{self.peer_id}] No tracker found. Starting election.")
                self._start_election_if_needed(ns)
        except Exception as e:
            print(f"[{self.peer_id}] Error in tracker discovery: {e}")
            
        ns._pyroRelease()

    def _start_election_if_needed(self, ns=None):
        if ns is None:
            ns = locate_ns()
            local_ns = True
        else:
            local_ns = False
        
        try:
            entries = ns.list(prefix=TRACKER_NAME_PREFIX)
            if not entries:
                self._trigger_election()
        except Exception as e:
            print(f"[{self.peer_id}] Error checking for tracker: {e}")
        
        if local_ns:
            ns._pyroRelease()

    def _trigger_election(self):
        global GLOBAL_EPOCH, GLOBAL_EPOCH_LOCK
        with GLOBAL_EPOCH_LOCK:
            GLOBAL_EPOCH += 1
            epoch = GLOBAL_EPOCH
        
        print(f"[{self.peer_id}] Starting election for epoch {epoch}")
        
        ns = locate_ns()
        entries = ns.list(prefix=TRACKER_NAME_PREFIX)
        if entries:
            latest_tracker = max(entries.keys(), key=lambda x: int(x.split('_')[-1]))
            self.tracker_uri = entries[latest_tracker]
            for name, uri in ns.list(prefix="Peer").items():
                if uri == self.tracker_uri:
                    self.tracker_peer_id = name
                    break
            print(f"[{self.peer_id}] Using existing tracker: {self.tracker_peer_id}")
            ns._pyroRelease()
            return
        
        peer_names = [n for n in ns.list().keys() if n.startswith("Peer")]
        votes = 1  
        self.voted_epochs.add(epoch)  
        
        for name in peer_names:
            if name == self.peer_id:
                continue
            try:
                proxy = Proxy(ns.lookup(name))
                if proxy.receive_vote_request(epoch, self.peer_id):
                    votes += 1
                proxy._pyroRelease()
            except Exception as e:
                print(f"[{self.peer_id}] Error requesting vote from {name}: {e}")
                continue
        
        majority = len(peer_names) // 2 + 1
        if votes >= majority:
            print(f"[{self.peer_id}] Won election with {votes} votes")
            self.become_tracker(epoch)
        else:
            print(f"[{self.peer_id}] Lost election with {votes} votes")
        
        ns._pyroRelease()

    def receive_vote_request(self, epoch, candidate_id):
        global GLOBAL_EPOCH
        if epoch < GLOBAL_EPOCH:
            return False
        if epoch in self.voted_epochs:
            return False
        self.voted_epochs.add(epoch)
        return True

    def become_tracker(self, epoch=None):
        self.is_tracker = True
        self.tracker_peer_id = self.peer_id
        tracker_instance = Tracker()
        uri = self.daemon.register(tracker_instance)
        
        global GLOBAL_EPOCH
        if epoch is None:
            epoch = GLOBAL_EPOCH
            
        tracker_name = f"{TRACKER_NAME_PREFIX}{epoch}"
        
        ns = locate_ns()
        try:
            ns.remove(tracker_name)  
        except Exception:
            pass
        ns.register(tracker_name, uri)
        ns._pyroRelease()
        
        self.tracker_uri = uri
        
        tracker_instance.register_peer(self.peer_id, self.files)
        print(f"[{self.peer_id}] Registered own files with self as tracker")

    def send_heartbeat(self):
        failed_heartbeats = {}
        
        while True:
            if self.is_tracker:
                try:
                    ns = locate_ns()
                    for name, uri in ns.list(prefix="Peer").items():
                        if name == self.peer_id:
                            continue
                        
                        if name in failed_heartbeats and failed_heartbeats[name] >= 3:
                            continue
                            
                        try:
                            proxy = Proxy(uri)
                            proxy.receive_heartbeat(self.peer_id)
                            proxy._pyroRelease()
                            if name in failed_heartbeats:
                                del failed_heartbeats[name]
                        except Exception as e:
                            failed_heartbeats[name] = failed_heartbeats.get(name, 0) + 1
                            
                            if failed_heartbeats[name] >= 3:
                                if name in self.active_peers:
                                    del self.active_peers[name]
                            else:
                                print(f"[{self.peer_id}] Failed to send heartbeat to {name}: {e}")
                    ns._pyroRelease()
                except Exception as e:
                    print(f"[{self.peer_id}] Error in heartbeat send: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def receive_heartbeat(self, sender_id):
        self.active_peers[sender_id] = time.time()
        self.active_peers[self.peer_id] = time.time()
        
        if not self.is_tracker and sender_id == self.tracker_peer_id:
            with self.tracker_timer_lock:
                self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX) + time.time()

    def monitor_heartbeat(self):
        while True:
            now = time.time()
            
            self.active_peers[self.peer_id] = now
            
            inactive = []
            for p, t in self.active_peers.items():
                if p != self.peer_id and now - t > TRACKER_TIMEOUT_MAX:
                    inactive.append(p)
            
            for p in inactive:
                del self.active_peers[p]
                
                if p == self.tracker_peer_id and not self.is_tracker:
                    self.tracker_uri = None
                    self.tracker_peer_id = None
            
            if not self.is_tracker and self.tracker_peer_id:
                with self.tracker_timer_lock:
                    if now > self.timer_expiry:
                        self._start_election_if_needed()
                        self.timer_expiry = time.time() + random.uniform(TRACKER_TIMEOUT_MAX * 2, TRACKER_TIMEOUT_MAX * 3)
            
            if not self.is_tracker and not self.tracker_uri:
                if not hasattr(self, 'last_election_attempt') or now - self.last_election_attempt > 5:
                    self._start_election_if_needed()
                    self.last_election_attempt = now
            
            time.sleep(HEARTBEAT_INTERVAL * 2)

    def register_files_with_tracker(self):
        if not self.tracker_uri:
            print(f"[{self.peer_id}] No tracker to register files with.")
            return
        
        try:
            tracker = Proxy(self.tracker_uri)
            tracker.register_peer(self.peer_id, self.files)
            tracker._pyroRelease()
            
            if not self.is_tracker:
                with self.tracker_timer_lock:
                    self.timer_expiry = time.time() + random.uniform(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
        except Exception as e:
            print(f"[{self.peer_id}] Failed to register files with tracker: {e}")
            self.tracker_uri = None
            self.tracker_peer_id = None

    def update_active_peers(self):
        ns = locate_ns()
        self.active_peers = {p: time.time() for p in ns.list(prefix="Peer").keys()}
        ns._pyroRelease()

    def query_and_download(self, file_name):

        if file_name in self.files:
            print(f"[{self.peer_id}] You already own '{file_name}'.")
            return

        if not self.tracker_uri:
            print(f"[{self.peer_id}] Tracker not available. Looking up again...")
            self.tracker_uri = self._lookup_tracker()
            if not self.tracker_uri:
                print(f"[{self.peer_id}] Still no tracker available.")
                return
        
        try:
            tracker = Proxy(self.tracker_uri)
            owners = tracker.query_file(file_name)
            print(f"[{self.peer_id}] File '{file_name}' found on peers: {owners}")
            tracker._pyroRelease()
        except Exception as e:
            print(f"[{self.peer_id}] Tracker query error: {e}")
            self.tracker_uri = None  
            return
        
        if not owners:
            print(f"[{self.peer_id}] File '{file_name}' not found on any peer.")
            return
            
        peer_to_contact = random.choice(owners)
        print(f"[{self.peer_id}] Attempting to download from {peer_to_contact}")
        
        try:
            ns = locate_ns()
            peer_uri = ns.lookup(peer_to_contact)
            print(f"[{self.peer_id}] Found URI for {peer_to_contact}: {peer_uri}")
            peer_proxy = Proxy(peer_uri)
            content = peer_proxy.get_file(file_name)
            peer_proxy._pyroRelease()
            ns._pyroRelease()
        except Exception as e:
            print(f"[{self.peer_id}] Error contacting {peer_to_contact}: {e}")
            return
        
        if content is None:
            print(f"[{self.peer_id}] File not available on peer {peer_to_contact}.")
            return
        
        file_path = os.path.join("files", file_name)
        try:
            with open(file_path, "w") as f:
                f.write(content)
            print(f"[{self.peer_id}] Successfully downloaded '{file_name}'.")
            
            if file_name not in self.files:
                self.files.append(file_name)
                print(f"[{self.peer_id}] Added '{file_name}' to local file list.")
                self.register_files_with_tracker()
        except Exception as e:
            print(f"[{self.peer_id}] Error writing file: {e}")

    def get_file(self, file_name):
        print(f"[{self.peer_id}] Received request for file: {file_name}")
        path = os.path.join("files", file_name)
        if os.path.exists(path):
            with open(path, "r") as f:
                content = f.read()
                print(f"[{self.peer_id}] Sending file content for: {file_name}")
                return content
        print(f"[{self.peer_id}] File not found: {file_name}")
        return None

    def _load_local_files(self):
        files_dir = os.path.join(os.getcwd(), "files")
        all_files = os.listdir(files_dir)

        def num(fn):
            m = re.search(r"\d+", fn)
            return int(m.group()) if m else None

        numbers = [n for n in (num(f) for f in all_files) if n]
        peer_num = int(re.search(r"\d+", self.peer_id).group())

        def is_prime(n):
            return n in {1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47}

        if peer_num == 1:
            sel = [n for n in numbers if n % 2 == 0]
        elif peer_num == 2:
            sel = [n for n in numbers if n % 3 == 0]
        elif peer_num == 3:
            sel = [n for n in numbers if n % 4 == 0]
        elif peer_num == 4:
            sel = [n for n in numbers if n % 5 == 0]
        elif peer_num == 5:
            sel = [n for n in numbers if is_prime(n)]
        else:
            sel = []

        return [f"file{n}.txt" for n in sel if os.path.exists(os.path.join(files_dir, f"file{n}.txt"))]