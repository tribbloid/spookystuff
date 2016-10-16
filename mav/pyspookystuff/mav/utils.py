import os

from multiprocessing import Lock

lock = Lock()

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