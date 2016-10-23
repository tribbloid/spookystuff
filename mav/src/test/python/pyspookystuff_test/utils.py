
import os

import multiprocessing

# CAUTION: avoid using ANY multiprocessing component in production code!
# process safety relies on Linux subprocess inheritance,
# which doesn't exist on Windows or if process is created by PythonDriver
# this causes erratic conflicts and should be eliminated AT ALL COST!
mpManager = multiprocessing.Manager()

lock = multiprocessing.Lock()

def nextUnused(existing, candidates, blacklist=list()):
    global lock

    lock.acquire()
    combined = existing + blacklist
    for i in candidates:
        if i not in combined:
            existing.append(i)
            lock.release()
            return i

    lock.release()
    os.error("Depleted: running dry!")

    # TODO: customize error info
    # raise mav.DronePoolDepletedException(
    #     "All drones are dispatched or unreachable:\n" +
    #     "dispatched:\n" +
    #     json.dumps(Endpoint.used) + "\n" +
    #     "unreachable:\n" +
    #     json.dumps(Endpoint.unreachable)
    # )

    #
    # class LazyWrapper(object):
    #
    #     def __init__(self, fn):
    #         self.fn = fn
    #
    #     def get(self):
    #         self.fn()