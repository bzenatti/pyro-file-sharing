import re
import threading
import time
from Pyro5.api import Daemon
from services.peer import Peer
from common.utils import get_daemon_and_ns
import os


def clear_terminal():
   os.system('cls' if os.name == 'nt' else 'clear')


def peer_menu(peer):
    while True:
        print("\nPeer Menu:")
        print("1. List available files")
        print("2. Query and download a file")
        print("3. View active peers")
        print("4. Exit")

        choice = input("Enter your choice: ")
        
        if choice == '1':
            print(f"\nAvailable files: {peer.files}")
        elif choice == '2':
            file_name = input("Enter the name of the file to download: ")
            peer.query_and_download(file_name)
        elif choice == '3':
            peer.update_active_peers()
            print(f"\nActive peers: {list(peer.active_peers.keys())}")
        elif choice == '4':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please try again.")

def main(peer_id):
    clear_terminal()
    peer = Peer(peer_id)
    
    # Create and start the menu thread first
    menu_thread = threading.Thread(target=peer_menu, args=(peer,), daemon=True)
    menu_thread.start()
    
    # Start the peer process
    try:
        peer.start()  # This will block with request loop
    except KeyboardInterrupt:
        print("Shutting down...")
    
    # Wait for menu thread to complete
    menu_thread.join(timeout=1)

if __name__ == "__main__":
   print("Enter the peer ID (e.g., Peer1, Peer2, Peer3, Peer4, Peer5):")
   peer_id = input()

   match = re.search(r"Peer\d", peer_id)
   if match:
       main(peer_id)
   else:
       print("Invalid input. Please enter a valid peer name like 'Peer3'.")