from Pyro5.api import expose

@expose
class Tracker:
    def __init__(self):
        self.file_index = {}
        self.peers = {}

    def register_peer(self, peer_id, file_list):
        self.peers[peer_id] = file_list
        for file in file_list:
            if file not in self.file_index:
                self.file_index[file] = []
            if peer_id not in self.file_index[file]:
                self.file_index[file].append(peer_id)
        print(f"[Tracker] Registered peer {peer_id} with files: {file_list}")

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

    def heartbeat(self):
        return True

    def ping(self):
        pass

    def log_file_request(self, peer_id, file_name):
        print(f"[Tracker] Peer {peer_id} requested file '{file_name}' which is not available.")