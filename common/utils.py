import random
import time
import os
import re
from Pyro5.api import locate_ns, Daemon

FILES_DIR = os.path.join(os.getcwd(), "files")

def generate_epoch():
    return int(time.time())

def start_timer_with_random_interval(min_timeout, max_timeout):
    return time.time() + random.uniform(min_timeout, max_timeout)

def get_daemon_and_ns():
    daemon = Daemon()
    ns = locate_ns()
    return daemon, ns

def load_local_files(peer_id: str) -> list[str]:
    all_files = os.listdir(FILES_DIR)

    def num(fn: str):
        m = re.search(r"\d+", fn)
        return int(m.group()) if m else None

    numbers = [n for n in (num(f) for f in all_files) if n]
    peer_num = int(re.search(r"\d+", peer_id).group())

    def is_prime(n: int) -> bool:
        return n in {1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47}

    if peer_num == 1:
        sel = [n for n in numbers if n % 2 == 0]
    elif peer_num == 2:
        sel = [n for n in numbers if n % 3 == 0]
    elif peer_num == 3:
        sel = [n for n in numbers if n % 4 == 0]
    elif peer_num == 4:
        sel = [n for n in numbers if n % 5 == 0]
    elif peer_num == 5:
        sel = [n for n in numbers if is_prime(n)]
    else:
        sel = []

    return [
        f"file{n}.txt"
        for n in sel
        if os.path.exists(os.path.join(FILES_DIR, f"file{n}.txt"))
    ]


def read_file(file_name: str) -> str | None:
    path = os.path.join(FILES_DIR, file_name)
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read()
    return None


def write_file(file_name: str, content: str) -> None:
    path = os.path.join(FILES_DIR, file_name)
    with open(path, "w") as f:
        f.write(content)