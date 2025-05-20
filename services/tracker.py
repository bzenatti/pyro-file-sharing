from Pyro5.api import expose, locate_ns, Proxy
import threading
import time

from common.config import HEARTBEAT_INTERVAL

@expose
class Tracker:
    def __init__(self, tracker_peer_id):
        self.tracker_peer_id = tracker_peer_id
        self.file_index: dict[str, list[str]] = {}
        self.peers: dict[str, list[str]] = {}
        self._running = True
        self._last_seen: dict[str, float] = {}
        self._hb_thread = threading.Thread(target=self._heartbeat_loop,daemon=True)
        self._hb_thread.start()

    def register_peer(self, peer_id, file_list):
        self.peers[peer_id] = file_list
        for file in file_list:
            if file not in self.file_index:
                self.file_index[file] = []
            if peer_id not in self.file_index[file]:
                self.file_index[file].append(peer_id)
        print(f"\n[Tracker] Registered peer {peer_id} with files: {file_list}")

    def update_peer_file_list(self, peer_id, file_list):
        old_files = self.peers.get(peer_id, [])
        for file in old_files:
            if file in self.file_index and peer_id in self.file_index[file]:
                self.file_index[file].remove(peer_id)
                if not self.file_index[file]:
                    del self.file_index[file]

        self.register_peer(peer_id, file_list)

    def query_file(self, file_name):
        return self.file_index.get(file_name, [])

    def log_file_request(self, peer_id, file_name):
        print(f"\n[Tracker] Peer {peer_id} requested file '{file_name}' which is not available.")

    def remove_peer(self, peer_id):
       if peer_id in self.peers:
           del self.peers[peer_id]
       for file, owners in list(self.file_index.items()):
           if peer_id in owners:
               owners.remove(peer_id)
               if not owners:
                   del self.file_index[file]
       print(f"\n[Tracker] Removed peer {peer_id}")

    def ping(self):
        pass
    def _heartbeat_loop(self):
        while self._running:
            try:
                ns = locate_ns()
                peers = ns.list(prefix="Peer")
                ns._pyroRelease()
                
                for name, uri in peers.items():
                    try:
                        proxy = Proxy(uri)
                        result = proxy.receive_tracker_heartbeat(self.tracker_peer_id)
                        if not result:
                            print(f"\n[Tracker] Failed to send heartbeat to {name}")
                        proxy._pyroRelease()
                    except Exception as e:
                        print(f"\n[Tracker] Error sending heartbeat to {name}: {e}")
            except Exception as e:
                print(f"\n[Tracker] Heartbeat loop error: {e}")
            time.sleep(HEARTBEAT_INTERVAL)
    
    def get_tracker_peer_id(self):
        return self.tracker_peer_id
