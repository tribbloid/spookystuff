import ctypes
import json
import os
import random
import multiprocessing

import dronekit
from datetime import datetime

from pyspookystuff import mav


# pool = dict([])
# instance: dict -> InstancInfo
# used by daemon to poll, new element is inserted by Binding creation.

# clusterActiveInstances = [] merged into pool
# won't be polled as they are bind to other workers.
# CAUTION! this shouldn't contain any non-unique connStr, e.g. tcp:localhost:xxx, serial:xxx


# all created bindings, each thread can have max 1 binding at a time and
# is destroyed once the vehicle is lost or returned to the pool.
# Non-active bindings are not allowed as they take precious proxy ports

# lastUpdates = [] merged into pool
# updated by both binding process and daemon at the same time
# memorize last telemetric data of each vehicle

# class IStatus:
#     # not mode.
#     Idle, Active, Missing, Grounded = range(4)
#
# class IInfo(object):
#     """values of pool, status=Idle when initialized, if connection fail status=Missing, after which it
#     will only be connected by poll daemon
#     """
#     # connStr = ""
#     # def isGlobal(self):
#     #     # type: () -> object
#     #     # TODO: are your sure?
#     #     if self.connStr.startswith("serial"): return False
#     #     else: return True
#
#     # vehicleClass = None
#
#     def __init__(self):
#         self.status = IStatus.Idle
#         self.lastUpdated = datetime.now()
#         self.lastError = None


class Instance(object):
    all = multiprocessing.Array(ctypes.c_char_p, 10)  # type: multiprocessing.Array
    used = multiprocessing.Array(ctypes.c_char_p, 10)  # type: multiprocessing.Array
    missing = multiprocessing.Array(ctypes.c_char_p, 10)  # type: multiprocessing.Array

    # will be tried by daemon if not in usedConnStr
    # won't be tried by nobody
    # won't be tried by executor, daemon will still try it and if successful, will remove it from the list
    # in all these arrays json strings of Instances are stored. This is the only way to discover duplicity

    def __init__(self, _json):
        # type: (str) -> None
        self.json = _json
        _dict = json.loads(_json)
        self.endpoint = _dict['connectionString']
        self.vClass = _dict['vehicleClass']

    def isNotUsed(self):
        result = not (self.json in self.used)
        return result

    def isAvailable(self):
        result = not (self.json in self.used + self.missing)
        return result


usedPort = multiprocessing.Array(ctypes.c_long, 10)  # type: multiprocessing.Array


# won't be used as new proxy port

class ProxyFactory(object):
    def __init__(self, _json):
        _dict = json.loads(_json)
        self.ports = _dict['ports']
        self.gcsMapping = _dict['gcsMapping']
        self.polling = _dict['polling']
        self.name = _dict['name']

    def findPort(self):
        for port in self.ports:
            if not (port in usedPort): return port

        raise mav.PortDepletedException("all ports for proxies are used")


class Proxy(object):
    global usedPort

    def __init__(self, instance, name, port, outs):
        self.port = port
        self.outs = outs
        self.name = name
        self.endpoint = 'localhost:' + str(port)

        effectiveOuts = self.outs + [self.endpoint]
        self.spawn = mav.proxyUp(aircraft=self.name, master=instance.connStr, outs=effectiveOuts)
        usedPort.append(self.port)

    def close(self):
        os.killpg(self.spawn.pid, 2)
        usedPort.remove(self.port)
        # TODO: cleanup variables to fail early?


# if all instances are not available, sleep for x seconds and retry.
class Binding(object):
    # local to process
    existing = None  # type: Binding

    @staticmethod
    def getOrCreate(instances, proxyFactory, polling=False):
        # type: (list[Instance], ProxyFactory, bool) -> Binding
        global existing
        if not existing:
            existing = Binding.create(instances, proxyFactory, polling)
        return existing

    @staticmethod
    def create(instances, proxyFactory, polling=False):
        # type: (list[Instance], ProxyFactory, bool) -> Binding

        # insert if not exist
        for ii in instances:
            _json = ii.json
            if not (_json in Instance.all):
                Instance.all.append(_json)

        # _is = instances[:] TODO: this enforce priority among drones in the pool, is it necessary?
        # random.shuffle(_is)

        _proxyFactory = proxyFactory if (polling or proxyFactory.polling) else None

        # iterate ONCE until a vehicle can be created
        # otherwise RAISE ERROR IMMEDIATELY! action will retry it locally and spark will retry it cluster-wise.
        # special polling-based GenPartitioner should be used to minimize error rate.
        for ii in instances:
            if ii.isAvailable():
                proxy = None
                try:
                    endpoint = ii.connStr
                    if _proxyFactory:
                        proxy = Proxy(
                            ii,
                            _proxyFactory.findPort(),
                            _proxyFactory.gcsMapping,
                            _proxyFactory.name
                        )
                        endpoint = proxy.endpoint

                    vehicle = dronekit.connect(
                        ip=endpoint,
                        wait_ready=True
                    )
                    binding = Binding(ii, proxy, vehicle)
                    return binding

                except Exception as ee:
                    Instance.missing.append(ii.json)

                finally:
                    if proxy: proxy.close()

        raise mav.DronePoolDepletedException(
            "All drones are dispatched or unreachable:\n" +
            "dispatched:\n" +
            json.dumps(Instance.used) + "\n" +
            "unreachable:\n" +
            json.dumps(Instance.missing)
        )

    def __init__(self, instance, proxy, vehicle):
        # type: (Instance, Proxy, dronekit.Vehicle) -> None
        self.instance = instance
        self.proxy = proxy
        self.vehicle = vehicle

        Instance.used.append(instance.json)

    def close(self):
        self.vehicle.close()
        self.proxy.close()

        Instance.used.remove(self.instance.json)
