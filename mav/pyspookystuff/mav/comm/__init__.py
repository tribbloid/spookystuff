import json
import os

import dronekit
import re
import sys

from pyspookystuff.mav import utils
from pyspookystuff import mav

# pool = dict([])
# endpoint: dict -> InstancInfo
# used by daemon to poll, new element is inserted by Binding creation.

# clusterActiveEndpoints = [] merged into pool
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

# bean project
# not consistent with other resource allocation mechanism.
class Endpoint(object):
    # static variables shared by all processes
    # all = multiprocessing.Array(ctypes.c_char_p, 10)  # type: multiprocessing.Array
    all = mav.manager.list()
    # will be tried by daemon if not in used

    # used = multiprocessing.Array(ctypes.c_char_p, 10)  # type: multiprocessing.Array
    used = mav.manager.list()
    # won't be tried by nobody

    # unreachable = multiprocessing.Array(ctypes.c_char_p, 10)  # type: multiprocessing.Array
    unreachable = mav.manager.list()

    # won't be tried by executor, daemon will still try it and if successful, will remove it from the list
    # in all these arrays json strings of Endpoints are stored. This is the only way to discover duplicity

    # TODO: use scala reflection to have a unified interface.
    @staticmethod
    def fromJSON(_json):
        # type: (str) -> Endpoint
        _dict = json.loads(_json)
        Endpoint(_dict['uris'], _dict['vehicleClass'])

    # TODO: use **local() to reduce boilerplate copies
    def __init__(self, uris, vehicleClass=None):
        self.uris = uris
        self.vehicleClass = vehicleClass

    @property
    def _connStrNoInit(self):
        return self.uris[0]

    @staticmethod
    def nextUnused():
        # type: () -> Endpoint
        utils.nextUnused(Endpoint.used, Endpoint.all)

    @staticmethod
    def nextImmediatelyAvailable(all):
        # type: () -> Endpoint

        utils.nextUnused(Endpoint.used, all, Endpoint.unreachable)


class ProxyFactory(object):

    @staticmethod
    def fromJSON(_json):
        _dict = json.loads(_json)
        ProxyFactory(_dict['ports'], _dict['gcsMapping'], _dict['polling'])

    def __init__(self, ports=range(12014,12108), gcsMapping=None, polling=False):
        # type: (list, dict, boolean) -> None
        if gcsMapping is None:
            gcsMapping = {'.*': ['127.0.0.1:14550']}

        self.ports = ports
        self.gcsMapping = gcsMapping
        self.polling = polling

    def nextPort(self):  # NOT static! don't move anywhere
        port = utils.nextUnused(Proxy.usedPort, self.ports)
        return port

    def nextProxy(self, connStr, vType=None):
        port = self.nextPort()
        outs = None

        for k in self.gcsMapping:
            if re.match(k, connStr):
                outs = self.gcsMapping[k]

        if not vType:
            name = connStr
        else:
            name = vType + ":" + connStr

        if outs:
            try:
                proxy = Proxy(
                    connStr,
                    name,
                    port,
                    outs
                )
                return proxy
            except Exception:
                if port in Proxy.usedPort:
                    Proxy.usedPort.remove(port)
                raise
        else:
            return None


class Proxy(object):
    usedPort = mav.manager.list()
    # usedPort = multiprocessing.Array(ctypes.c_long, 10, lock=True)  # type: multiprocessing.Array

    @staticmethod
    def _up(aircraft, setup=False, master='tcp:127.0.0.1:5760', outs={'127.0.0.1:14550'},
            options=None, logfile=sys.stdout):
        """
        launch mavproxy connected to a SIL endpoint
        mavproxy.py -h
        Usage: mavproxy.py [options]

        Options:
          -h, --help            show this help message and exit
          --master=DEVICE[,BAUD]
                                MAVLink master port and optional baud rate
          --out=DEVICE[,BAUD]   MAVLink output port and optional baud rate
          --baudrate=BAUDRATE   default serial baud rate
          --sitl=SITL           SITL output port
          --streamrate=STREAMRATE
                                MAVLink stream rate
          --source-system=SOURCE_SYSTEM
                                MAVLink source system for this GCS
          --source-component=SOURCE_COMPONENT
                                MAVLink source component for this GCS
          --target-system=TARGET_SYSTEM
                                MAVLink target master system
          --target-component=TARGET_COMPONENT
                                MAVLink target master component
          --logfile=LOGFILE     MAVLink master logfile
          -a, --append-log      Append to log files
          --quadcopter          use quadcopter controls
          --setup               start in setup mode
          --nodtr               disable DTR drop on close
          --show-errors         show MAVLink error packets
          --speech              use text to speech
          --aircraft=AIRCRAFT   aircraft name
          --cmd=CMD             initial commands
          --console             use GUI console
          --map                 load map module
          --load-module=LOAD_MODULE
                                Load the specified module. Can be used multiple times,
                                or with a comma separated list
          --mav09               Use MAVLink protocol 0.9
          --mav20               Use MAVLink protocol 2.0
          --auto-protocol       Auto detect MAVLink protocol version
          --nowait              don't wait for HEARTBEAT on startup
          -c, --continue        continue logs
          --dialect=DIALECT     MAVLink dialect
          --rtscts              enable hardware RTS/CTS flow control
          --moddebug=MODDEBUG   module debug level
          --mission=MISSION     mission name
          --daemon              run in daemon mode, do not start interactive shell
          --profile             run the Yappi python profiler
          --state-basedir=STATE_BASEDIR
                                base directory for logs and aircraft directories
          --version             version information
          --default-modules=DEFAULT_MODULES
                                default module list
        """
        import pexpect
        MAVPROXY = os.getenv('MAVPROXY_CMD', 'mavproxy.py')
        cmd = MAVPROXY + ' --master=%s' % master
        for out in outs:
            cmd += ' --out=%s' % out
        if setup:
            cmd += ' --setup'
        cmd += ' --aircraft=%s' % aircraft
        if options is not None:
            cmd += ' ' + options
        ret = pexpect.spawn(cmd, logfile=logfile, timeout=60)
        ret.delaybeforesend = 0
        return ret

    def __init__(self, connStr, name, port, outs):

        # primary out, always localhost
        self.port = port

        # auxiliary outs
        self.outs = outs

        # affect dir of log files
        self.name = name

        effectiveOuts = self.outs + [self.uri]
        self.spawn = Proxy._up(aircraft=self.name, master=connStr, outs=effectiveOuts)

    @property
    def uri(self):
        return 'localhost:' + str(self.port)

    def close(self):
        os.killpg(self.spawn.pid, 2)
        if self.port in Proxy.usedPort:
            Proxy.usedPort.remove(self.port)
            # TODO: cleanup variables to fail early?


# if all endpoints are not available, sleep for x seconds and retry.
class Connection(object):
    # process local, 1 process can only have 1 binding.
    existing = None  # type: Connection

    @staticmethod
    def getOrCreate(endpoints, proxyFactory, polling=False):
        # type: (list, ProxyFactory, bool) -> Connection
        if not Connection.existing:
            existing = Connection.create(endpoints, proxyFactory, polling)
        return Connection.existing

    @staticmethod
    def create(endpoints, proxyFactory, polling=False):
        # type: (list[Endpoint], ProxyFactory, bool) -> Connection

        factory = proxyFactory if (polling or proxyFactory.polling) else None

        # iterate ONCE until a vehicle can be created
        # otherwise RAISE ERROR IMMEDIATELY! action will retry it locally and spark will retry it cluster-wise.
        # special polling-based GenPartitioner should be used to minimize error rate.
        endpoint = Endpoint.nextImmediatelyAvailable(endpoints)
        proxy = None
        vehicle = None
        try:
            uri = endpoint._connStrNoInit
            if factory:
                proxy = factory.nextProxy(endpoint._connStrNoInit)
                uri = proxy.uri()

            vehicle = dronekit.connect(
                ip=uri,
                wait_ready=True
            )
            binding = Connection(endpoint, proxy, vehicle)
            return binding

        except Exception:
            Endpoint.unreachable.append(endpoint)
            Endpoint.used.remove(endpoint)
            if proxy:
                proxy.close()
            if vehicle:
                vehicle.close()
            raise

    def __init__(self, endpoint, proxy, vehicle):
        # type: (Endpoint, Proxy, dronekit.Vehicle) -> None
        self.endpoint = endpoint
        self.proxy = proxy
        self.vehicle = vehicle

    def close(self):
        Endpoint.used.remove(self.endpoint)
        self.vehicle.close()
        self.proxy.close()
