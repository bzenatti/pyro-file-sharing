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
        self.files = load_local_files(self.peer_id)
        self.tracker_uri = None
        self.tracker_peer_id = None
        self.voted_epochs = set()
        self.is_tracker = False
        self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
        self.tracker_timer_lock = threading.Lock()
        self.active_peers = {}
        self.daemon = None
        self.ns = None
        self.running = True
        self.threads = []

    def start(self):
        self.daemon, self.ns = get_daemon_and_ns()
        uri = self.daemon.register(self)
        self.ns.register(self.peer_id, uri)
        print(f"\n[{self.peer_id}] Registered in Name Server with URI: {uri}")
    
        # Start background threads
        thread1 = threading.Thread(target=self.tracker_discovery_and_election, daemon=True)
        thread1.start()
        self.threads.append(thread1)
        
        thread2 = threading.Thread(target=self.monitor_heartbeat, daemon=True)
        thread2.start()
        self.threads.append(thread2)
        
        thread3 = threading.Thread(target=self.send_heartbeat, daemon=True)
        thread3.start()
        self.threads.append(thread3)
    
    def run_daemon(self):
        if self.daemon:
            self.daemon.requestLoop(loopCondition=lambda: self.running)
            print(f"\n[{self.peer_id}] Daemon loop exited")

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
            print(f"\n[{self.peer_id}] Error during tracker lookup: {e}")
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
                        
                print(f"\n[{self.peer_id}] Found tracker: {self.tracker_uri}")
                self.register_files_with_tracker()
            else:
                print(f"\n[{self.peer_id}] No tracker found. Starting election.")
                self._start_election_if_needed(ns)
        except Exception as e:
            print(f"\n[{self.peer_id}] Error in tracker discovery: {e}")
            
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
            print(f"\n[{self.peer_id}] Error checking for tracker: {e}")
        
        if local_ns:
            ns._pyroRelease()

    def _trigger_election(self):
        global GLOBAL_EPOCH, GLOBAL_EPOCH_LOCK
        with GLOBAL_EPOCH_LOCK:
            GLOBAL_EPOCH += 1
            epoch = GLOBAL_EPOCH
        
        print(f"\n[{self.peer_id}] Starting election for epoch {epoch}")
        
        ns = locate_ns()
        entries = ns.list(prefix=TRACKER_NAME_PREFIX)
        if entries:
            latest_tracker = max(entries.keys(), key=lambda x: int(x.split('_')[-1]))
            self.tracker_uri = entries[latest_tracker]
            for name, uri in ns.list(prefix="Peer").items():
                if uri == self.tracker_uri:
                    self.tracker_peer_id = name
                    break
            print(f"\n[{self.peer_id}] Using existing tracker: {self.tracker_peer_id}")
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
            print(f"\n[{self.peer_id}] Won election with {votes} votes")
            self.become_tracker(epoch)
        else:
            print(f"\n[{self.peer_id}] Lost election with {votes} votes")
        
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
        tracker_instance = Tracker(self.peer_id)
        self.tracker_instance = tracker_instance
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
        print(f"\n[{self.peer_id}] Became tracker (epoch {epoch}).")
        print(f"\n[{self.peer_id}] Registered own files with self as tracker")

    def send_heartbeat(self):
            failed_to_tracker = 0          #for testing with more than one try
            while self.running:
                try:
                    ns = locate_ns()
                    if not self.is_tracker and self.tracker_uri:
                        try:
                            tracker_proxy = Proxy(self.tracker_uri)
                            tracker_proxy.receive_heartbeat(self.peer_id)
                            tracker_proxy._pyroRelease()
                            failed_to_tracker = 0          
                        except Exception as e:
                            failed_to_tracker += 1
                            if failed_to_tracker >= 1:
                                print(f"\n[{self.peer_id}] Tracker unreachable on SEND HB. Clearing tracker info and triggering election.") 
                                self.tracker_uri = None
                                self.tracker_peer_id = None
                                failed_to_tracker = 0
                                
                            else:
                                print(f"[{self.peer_id}] Failed HB to tracker: {e}")

                    for name, uri in ns.list(prefix="Peer").items():
                        if name == self.peer_id:
                            continue
                        try:
                            proxy = Proxy(uri)
                            proxy.receive_heartbeat(self.peer_id)
                            proxy._pyroRelease()
                        except Exception:
                            pass  

                    ns._pyroRelease()
                except Exception as e:
                    print(f"\n[{self.peer_id}] Heartbeat send error: {e}")

                time.sleep(HEARTBEAT_INTERVAL)

    def receive_heartbeat(self, sender_id):
        self.active_peers[sender_id] = time.time()
        self.active_peers[self.peer_id] = time.time()

        if not self.is_tracker and sender_id == self.tracker_peer_id:
            with self.tracker_timer_lock:
                self.timer_expiry = start_timer_with_random_interval(
                    TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX
                )
        return True          

    def monitor_heartbeat(self):
        while self.running:
            now = time.time()
            
            self.active_peers[self.peer_id] = now
            
            # Check for inactive peers
            inactive = []
            for p, t in self.active_peers.items():
                if p != self.peer_id and now - t > TRACKER_TIMEOUT_MAX * 2:
                    inactive.append(p)
            
            for p in inactive:
                print(f"[{self.peer_id}] Peer {p} is inactive and will be removed")
                del self.active_peers[p]
                
                # If the tracker went down, clear tracker info and trigger election
                if p == self.tracker_peer_id and not self.is_tracker:
                    print(f"\n[{self.peer_id}] Tracker {p} is down, clearing tracker info")
                    self.tracker_uri = None
                    self.tracker_peer_id = None
                    self._start_election_if_needed()
            
            # Check tracker timeout
            if not self.is_tracker and self.tracker_peer_id:
                with self.tracker_timer_lock:
                    if now > self.timer_expiry:
                        print(f"\n[{self.peer_id}] Tracker timeout occurred. Starting election.")
                        self.tracker_uri = None
                        self.tracker_peer_id = None
                        self._start_election_if_needed()
                        self.timer_expiry = time.time() + random.uniform(TRACKER_TIMEOUT_MAX * 2, TRACKER_TIMEOUT_MAX * 3)
            
            # If no tracker exists, try election
            if not self.is_tracker and not self.tracker_uri:
                if not hasattr(self, 'last_election_attempt') or now - self.last_election_attempt > 5:
                    self._start_election_if_needed()
                    self.last_election_attempt = now
        
            time.sleep(HEARTBEAT_INTERVAL * 2)

    def register_files_with_tracker(self):
        if not self.tracker_uri:
            print(f"\n[{self.peer_id}] No tracker to register files with.")
            return
        
        try:
            tracker = Proxy(self.tracker_uri)
            tracker.register_peer(self.peer_id, self.files)
            tracker._pyroRelease()
            
            if not self.is_tracker:
                with self.tracker_timer_lock:
                    self.timer_expiry = time.time() + random.uniform(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)
        except Exception as e:
            print(f"\n[{self.peer_id}] Failed to register files with tracker: {e}")
            self.tracker_uri = None
            self.tracker_peer_id = None

    def update_active_peers(self):
        ns = locate_ns()
        self.active_peers = {p: time.time() for p in ns.list(prefix="Peer").keys()}
        ns._pyroRelease()

    def query_and_download(self, file_name):
        if file_name in self.files:
            print(f"\n[{self.peer_id}] You already own '{file_name}'.")
            return
        
        #Who has the file?
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
            print(f"\n[{self.peer_id}] Error contacting {peer_to_contact}: {e}")
            return
        
        if content is None:
            print(f"\n[{self.peer_id}] File not available on peer {peer_to_contact}. Tracker error")
            return
        
        write_file(file_name, content)
        self.files.append(file_name)
        print(f"\n[{self.peer_id}] Added '{file_name}' to local file list.")
        self.register_files_with_tracker()

    def get_file(self, file_name):
        print(f"\n[{self.peer_id}] Received request for file: {file_name}")
        content = read_file(file_name)
        if content is not None:
            print(f"\n[{self.peer_id}] Sending file content for: {file_name}")
        else:
            print(f"\n[{self.peer_id}] File not found: {file_name}")
        return content
    
    def shutdown(self):
        if not self.running:
            return
        
        print(f"\n[{self.peer_id}] Initiating shutdown sequence...")
        self.running = False
        
        # Wait for threads to terminate
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        
        # Notify tracker if this peer is leaving and we're not the tracker
        if self.tracker_uri and not self.is_tracker:
            try:
                proxy = Proxy(self.tracker_uri)
                proxy.remove_peer(self.peer_id)
                proxy._pyroRelease()
                print(f"\n[{self.peer_id}] Notified tracker of departure")
            except Exception as e:
                print(f"\n[{self.peer_id}] Failed to notify tracker: {e}")
        
        # Close the daemon
        if self.daemon:
            try:
                self.daemon.close()
                print(f"\n[{self.peer_id}] Daemon closed")
            except Exception as e:
                print(f"\n[{self.peer_id}] Error closing daemon: {e}")
        
        # Remove entries from name server
        try:
            ns = locate_ns()
            
            # Remove peer entry
            try:
                ns.remove(self.peer_id)
                print(f"\n[{self.peer_id}] Removed from name server")
            except Exception as e:
                print(f"\n[{self.peer_id}] Error removing peer from name server: {e}")
            
            ns._pyroRelease()
        except Exception as e:
            print(f"\n[{self.peer_id}] Name server error: {e}")