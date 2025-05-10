# main.py
import re
import threading
import time
from Pyro5.api import Daemon, Proxy
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
        print("3. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            print(f"\nAvailable files: {peer.files}")
        elif choice == '2':
            file_name = input("Enter the name of the file to download: ")
            peer.query_and_download(file_name)
        elif choice == '3':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please try again.")

def main(peer_id):
    clear_terminal()
    daemon, ns = get_daemon_and_ns()
    peer = Peer(peer_id)
    # uri = daemon.register(peer) # Remove this line
    # ns.register(peer_id, uri) # Remove this line

    #print(f"{peer_id} running. URI: {uri}")

    # Start peer in a separate thread
    peer_thread = threading.Thread(target=peer.start, daemon=True)
    peer_thread.start()

    # Give the peer some time to start and register with the tracker
    time.sleep(2)

    # Interactive menu for the peer
    peer_menu(peer)

    # Clean up Pyro objects
    daemon.close()
    # ns.close() # Remove this line

if __name__ == "__main__":
    print("Enter the peer ID (e.g., Peer1, Peer2, Peer3, Peer4, Peer5):")
    peer_id = input()

    match = re.search(r"Peer\d", peer_id)
    if match:
        main(peer_id)
    else:
        print("Invalid input. Please enter a valid peer name like 'Peer3'.")