import re
import threading
import time
from Pyro5.api import Daemon
from services.peer import Peer
from common.utils import get_daemon_and_ns
import os
import signal


def clear_terminal():
   os.system('cls' if os.name == 'nt' else 'clear')


class PeerMenu(threading.Thread):
    def __init__(self, peer):
        super().__init__()
        self.peer = peer
        self.daemon = True  # Make it a daemon thread
        self._stop_event = threading.Event()
        
    def run(self):
        while not self._stop_event.is_set() and self.peer.running:
            print("\nPeer Menu:")
            print("1. List available files")
            print("2. Query and download a file")
            print("3. View active peers")
            print("4. Exit")

            try:
                choice = input("Enter your choice: ")
                
                if choice == '1':
                    print(f"\nAvailable files: {self.peer.files}")
                elif choice == '2':
                    file_name = input("Enter the name of the file to download: ")
                    self.peer.query_and_download(file_name)
                elif choice == '3':
                    self.peer.update_active_peers()
                    print(f"\nActive peers: {list(self.peer.active_peers.keys())}")
                elif choice == '4':
                    print("Exiting.")
                    self.peer.running = False
                    break
                else:
                    print("Invalid choice. Please try again.")
            except (KeyboardInterrupt, EOFError):
                self.peer.running = False
                break

    def stop(self):
        print("Stopping menu...")
        self._stop_event.set()


def main(peer_id):
    clear_terminal()
    peer = Peer(peer_id)
    peer.start()

    menu = PeerMenu(peer)
    menu.start()

    try:
        peer.run_daemon()
    except KeyboardInterrupt:
        print("\nInterrupted. Shutting down...")
    finally:
        peer.shutdown()
        menu.stop()
        print("Exited successfully")


if __name__ == "__main__":
   print("Enter the peer ID (e.g., Peer1, Peer2, Peer3, Peer4, Peer5):")
   peer_id = input()

   match = re.search(r"Peer\d", peer_id)
   if match:
       main(peer_id)
   else:
       print("Invalid input. Please enter a valid peer name like 'Peer3'.")