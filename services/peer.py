from Pyro5.api import expose, Proxy, locate_ns
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
        
        self.is_tracker = False
        self.last_heartbeat = time.monotonic()          
        self.current_timeout = random.uniform(TRACKER_TIMEOUT_MIN,TRACKER_TIMEOUT_MAX)     

        self.active_peers = {}
        self.voted_epochs = set()
        self.daemon = None
        self.ns = None
        self.running = True

        self.threads = []
        self.heartbeat_thread = None                      
        self.watchdog_thread = None                      
        self.election_lock = threading.Lock()          

    def start(self):
        self.daemon, self.ns = get_daemon_and_ns()
        uri = self.daemon.register(self)
        self.ns.register(self.peer_id, uri)
        print(f"\n[{self.peer_id}] Registered in Name Server with URI: {uri}")
        
        self.tracker_uri = self._lookup_tracker()
        
        t = threading.Thread(target=self.run_daemon, daemon=True)
        t.start()
        self.threads.append(t)

        self.watchdog_thread = threading.Thread(target=self.tracker_watchdog, daemon=True)
        self.watchdog_thread.start()
        self.threads.append(self.watchdog_thread)

        p = threading.Thread(target=self.send_heartbeat_peers, daemon=True)
        p.start()
        self.threads.append(p)
        
        if self.tracker_uri:
            try:
                self.tracker_peer_id = self._discover_tracker_peer_id()
                if self.tracker_peer_id:  
                    print(f"[{self.peer_id}] discovered tracker: {self.tracker_peer_id}")
                    tracker = Proxy(self.tracker_uri)
                    tracker.register_peer(self.peer_id, self.files)
                    tracker._pyroRelease()
                else:
                    print(f"[{self.peer_id}] No active tracker found, waiting for election")
            except Exception as e:
                print(f"[{self.peer_id}] Failed to register with tracker: {e}")
                self.tracker_uri = None  
    
    def run_daemon(self):
        if self.daemon:
            self.daemon.requestLoop(loopCondition=lambda: self.running)
    
    def receive_tracker_heartbeat(self, tracker_id):
        self.tracker_peer_id = tracker_id
        self.last_heartbeat  = time.monotonic()
        return True

    def _lookup_tracker(self):
        try:
            ns = locate_ns()
            entries = ns.list(prefix=TRACKER_NAME_PREFIX)
            if not entries:
                ns._pyroRelease()
                return None

            latest_epoch = max(int(name.split('_')[-1]) for name in entries)
            uri = entries[f"{TRACKER_NAME_PREFIX}{latest_epoch}"]
            ns._pyroRelease()

            return uri
        except Exception:
            return None

    def _discover_tracker_peer_id(self):
        try:
            tracker = Proxy(self.tracker_uri)
            tracker_id = tracker.get_tracker_peer_id()
            tracker._pyroRelease()
            return tracker_id
        except Exception as e:
            print(f"[{self.peer_id}] Error discovering tracker peer ID: {e}")
            return None

    def _trigger_election(self):
        if self.is_tracker:
            return  # already the tracker

        with self.election_lock:
            global GLOBAL_EPOCH
            current_epoch = self._get_current_epoch()
            new_epoch = current_epoch + 1
            
            print(f"\n[{self.peer_id}] Starting election for epoch {new_epoch}")

            try:
                ns = locate_ns()
                peers = [n for n in ns.list(prefix="Peer")]

                votes = 1  
                self.voted_epochs.add(new_epoch)

                for name in peers:
                    if name == self.peer_id:
                        continue
                    try:
                        proxy = Proxy(ns.lookup(name))
                        if proxy.receive_vote_request(new_epoch, self.peer_id):
                            votes += 1
                        proxy._pyroRelease()
                    except Exception:
                        pass 

                majority = len(peers) // 2 + 1
                if votes >= majority:
                    print(f"\n[{self.peer_id}] won election with {votes}/{len(peers)} votes")
                    self.become_tracker(new_epoch)
                    
                    self._broadcast_new_tracker(new_epoch)
                else:
                    print(f"\n[{self.peer_id}] lost election ({votes}/{len(peers)})")
                    self.current_timeout = random.uniform(TRACKER_TIMEOUT_MIN,TRACKER_TIMEOUT_MAX)    

                ns._pyroRelease()
            except Exception as e:
                print(f"[{self.peer_id}] election error: {e}")


    def receive_vote_request(self, epoch,candidate_id):
        global GLOBAL_EPOCH

        if epoch < GLOBAL_EPOCH:
            return False            

        if epoch in self.voted_epochs:
            return False            

        self.voted_epochs.add(epoch)

        if epoch > GLOBAL_EPOCH:
            with GLOBAL_EPOCH_LOCK:
                GLOBAL_EPOCH = epoch

        self.tracker_peer_id = candidate_id
        return True
    
    def tracker_watchdog(self):
        while self.running:
            time.sleep(0.01)

            if self.is_tracker:         
                continue

            elapsed = time.monotonic() - self.last_heartbeat
            if elapsed < self.current_timeout:
                self.current_timeout = random.uniform(TRACKER_TIMEOUT_MIN,TRACKER_TIMEOUT_MAX)    
                continue                

            self.last_heartbeat = time.monotonic()  
            self.current_timeout = random.uniform(TRACKER_TIMEOUT_MIN,TRACKER_TIMEOUT_MAX)
            self._trigger_election()

    def become_tracker(self, epoch: int = None):
        if self.is_tracker:
            return

        self.is_tracker = True
        self.tracker_peer_id = self.peer_id
        self.last_heartbeat = time.monotonic() 

        self.tracker_instance = Tracker(self.peer_id)
        uri = self.daemon.register(self.tracker_instance)

        global GLOBAL_EPOCH
        with GLOBAL_EPOCH_LOCK:
            if epoch is None:
                epoch = GLOBAL_EPOCH + 1
            
            if epoch > GLOBAL_EPOCH:
                GLOBAL_EPOCH = epoch

        tracker_name = f"{TRACKER_NAME_PREFIX}{epoch}"

        ns = locate_ns()
        ns.register(tracker_name, uri)
        ns._pyroRelease()

        self.tracker_uri = uri
        self.tracker_instance.register_peer(self.peer_id, self.files)
        self.voted_epochs = {epoch}
        print(f"[{self.peer_id}] became tracker (epoch {epoch})")

    def send_heartbeat_peers(self):
        while self.running:
            try:
                ns = locate_ns()
                now = time.monotonic()

                for name, uri in ns.list(prefix="Peer").items():
                    if name == self.peer_id:
                        self.active_peers[name] = now
                        continue
                    try:
                        proxy = Proxy(uri)
                        proxy.receive_heartbeat(self.peer_id)
                        proxy._pyroRelease()
                        self.active_peers[name] = now
                    except Exception:
                        pass
                ns._pyroRelease()
            except Exception as e:
                print(f"[{self.peer_id}] P2P heartbeat error: {e}")

            time.sleep(HEARTBEAT_INTERVAL)  

    def receive_heartbeat(self, sender_id: str) -> bool:
        self.active_peers[sender_id] = time.monotonic()
        return True        

    def _get_current_epoch(self):
        try:
            ns = locate_ns()
            entries = ns.list(prefix=TRACKER_NAME_PREFIX)
            ns._pyroRelease()
            
            if not entries:
                return 0
                
            return max(int(name.split('_')[-1]) for name in entries)
        except Exception:
            return 0

    def _broadcast_new_tracker(self, epoch):
        try:
            ns = locate_ns()
            peers = ns.list(prefix="Peer")
            ns._pyroRelease()
            
            for name, uri in peers.items():
                if name == self.peer_id:
                    continue
                try:
                    proxy = Proxy(uri)
                    proxy.update_tracker_info(self.tracker_uri, self.peer_id, epoch)
                    proxy._pyroRelease()
                except Exception as e:
                    print(f"\n[{self.peer_id}] Failed to notify {name} about new tracker: {e}")
        except Exception as e:
            print(f"\n[{self.peer_id}] Error broadcasting new tracker: {e}")

    def register_files_with_tracker(self):
        if not self.tracker_uri:
            print(f"\n[{self.peer_id}] no tracker, cannot register files")
            return
        try:
            proxy = Proxy(self.tracker_uri)
            proxy.register_peer(self.peer_id, self.files)
            proxy._pyroRelease()
        except Exception as e:
            print(f"\n[{self.peer_id}] failed to register files: {e}")
            self.tracker_uri     = None
            self.tracker_peer_id = None

    def update_active_peers(self):
        ns = locate_ns()
        self.active_peers = {p: time.time() for p in ns.list(prefix="Peer").keys()}
        ns._pyroRelease()
    
    def update_tracker_info(self, tracker_uri, tracker_peer_id, epoch):
        print(f"[{self.peer_id}] Received tracker update: {tracker_peer_id} (epoch {epoch})")
        self.tracker_uri = tracker_uri
        self.tracker_peer_id = tracker_peer_id
        self.last_heartbeat = time.monotonic()
        
        global GLOBAL_EPOCH
        with GLOBAL_EPOCH_LOCK:
            if epoch > GLOBAL_EPOCH:
                GLOBAL_EPOCH = epoch
        
        if not self.is_tracker:
            self.register_files_with_tracker()
        
        return True

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
        
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        
        if self.tracker_uri and not self.is_tracker:
            try:
                proxy = Proxy(self.tracker_uri)
                proxy.remove_peer(self.peer_id)
                proxy._pyroRelease()
                print(f"\n[{self.peer_id}] Notified tracker of departure")
            except Exception as e:
                print(f"\n[{self.peer_id}] Failed to notify tracker: {e}")
        
        if self.daemon:
            self.daemon.close()
        
        try:
            ns = locate_ns()
            ns.remove(self.peer_id)
            print(f"\n[{self.peer_id}] Removed from name server")
            ns._pyroRelease()
        except Exception as e:
            print(f"\n[{self.peer_id}] Name server error: {e}")