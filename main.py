import re
from Pyro5.api import Daemon
from services.peer import Peer
from common.utils import get_daemon_and_ns

def main(peer_id):
    daemon, ns = get_daemon_and_ns()
    peer = Peer(peer_id)
    uri = daemon.register(peer)
    ns.register(peer_id, uri)

    print(f"{peer_id} running. URI: {uri}")
    peer.start()

if __name__ == "__main__":
    print("Insert the name of the Peer with its number (1-5), like 'Peer1'")
    peer_id = input()

    match = re.search(r"\d+", peer_id)
    if match:
        main(peer_id)
    else:
        print("Invalid input. Please enter a valid peer name like 'Peer3'.")