import random
import time
from Pyro5.api import locate_ns, Daemon

def generate_epoch():
    return int(time.time())

def start_timer_with_random_interval(min_timeout, max_timeout):
    return time.time() + random.uniform(min_timeout, max_timeout)

def get_daemon_and_ns():
    daemon = Daemon()
    ns = locate_ns()
    return daemon, ns
