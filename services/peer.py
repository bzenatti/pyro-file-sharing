from Pyro5.api import expose, behavior, Daemon, locate_ns, Proxy
import os
import re
import threading
import random
import time

from common.config import *
from common.utils import *

@expose
class Peer:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.files = self._load_local_files()
        # print(self.files)
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

    def _load_local_files(self):
        """
        Loads files for this peer based on ID-specific rules:
        - Peer1: multiples of 2
        - Peer2: multiples of 3
        - Peer3: multiples of 4
        - Peer4: multiples of 5
        - Peer5: prime numbers and 1
        """
        files_dir = os.path.join(os.getcwd(), "files")
        all_files = os.listdir(files_dir)

        # Extract numbers from filenames
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

        # Convert back to filenames
        return [f"file{n}.txt" for n in selected if os.path.exists(os.path.join(files_dir, f"file{n}.txt"))]
