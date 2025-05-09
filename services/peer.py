from Pyro5.api import expose, behavior, Daemon, locate_ns, Proxy
import threading
import random
import time

from common.config import *
from common.utils import *

@expose
class Peer:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.files = [] 
        self.tracker_uri = None
        self.tracker = None
        self.epoch = 0
        self.voted_epochs = set()
        self.is_tracker = False
        self.timer_expiry = start_timer_with_random_interval(TRACKER_TIMEOUT_MIN, TRACKER_TIMEOUT_MAX)

    def start(self):
        #TODO - Locate Name Server
        #TODO - Register this peer
        #TODO - Try to find current tracker or start election
        #TODO - Start heartbeat listening thread
        pass

    def receive_vote_request(self, epoch, candidate_id):
        #TODO - Participate in election, vote if eligible
        return False

    def become_tracker(self):
        #TODO - Promote self to tracker role, create tracker instance, register in NS
        pass

    def monitor_heartbeat(self):
        #TODO - Periodically check for tracker heartbeat
        pass

    def register_files_with_tracker(self):
        #TODO - Notify tracker of current file list
        pass

    def query_and_download(self, file_name):
        #TODO - Ask tracker who has the file and initiate P2P download
        pass
