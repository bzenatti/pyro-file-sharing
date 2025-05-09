from Pyro5.api import expose

@expose
class Tracker:
    def __init__(self):
        self.file_index = {} 
        self.peers = {}       

    def register_peer(self, peer_id, file_list):
        #TODO - Register a peer and its files in the index
        pass

    def update_peer_file_list(self, peer_id, file_list):
        #TODO - Update file list for a peer
        pass

    def query_file(self, file_name):
        #TODO - Return a list of peers that have the given file
        return []

    def heartbeat(self):
        #TODO - send heartbeats 
        pass
