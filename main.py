import sys
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
    peer_id = sys.argv[1]
    main(peer_id)
