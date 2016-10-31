import os
import re
import sys

import dronekit

from pyspookystuff import utils


# print(sys.path)

class Endpoint(object):

    # won't be tried by executor, daemon will still try it and if successful, will remove it from the list
    # in all these arrays json strings of Endpoints are stored. This is the only way to discover duplicity

    # TODO: use **local() to reduce boilerplate copies
    def __init__(self, uris, vehicleClass=None):
        self.uris = uris
        self.vehicleClass = vehicleClass

    @property
    def _connStr(self):
        return self.uris[0]


class ProxyFactory(object):

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
    # process local
    existing = None  # type: Connection

    @staticmethod
    def getOrCreate(endpoints, proxyFactory, polling=False):
        # type: (list, ProxyFactory, bool) -> Connection
        if not Connection.existing:
            Connection.existing = Connection.create(endpoints, proxyFactory, polling)
        return Connection.existing

    @staticmethod
    def create(endpoints, proxyFactory, polling=False):
        # type: (list[Endpoint], ProxyFactory, bool) -> Connection

        factory = None
        if proxyFactory:
            if polling or proxyFactory.polling:
                factory = proxyFactory

        # iterate ONCE until a vehicle can be created
        # otherwise RAISE ERROR IMMEDIATELY! action will retry it locally and spark will retry it cluster-wise.
        # special polling-based GenPartitioner should be used to minimize error rate.
        endpoint = Endpoint.nextImmediatelyAvailable(endpoints)
        proxy = None
        vehicle = None
        try:
            uri = endpoint._connStr
            if factory:
                proxy = factory.nextProxy(endpoint._connStr)
                uri = proxy.uri()

            vehicle = dronekit.connect(
                ip=uri,
                wait_ready=True
            )
            binding = Connection(endpoint, proxy, vehicle)
            return binding

        except Exception:
            unreachableEndpoints.append(endpoint)
            usedEndpoints.remove(endpoint)
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
        usedEndpoints.remove(self.endpoint)
        self.vehicle.close()
        self.proxy.close()
