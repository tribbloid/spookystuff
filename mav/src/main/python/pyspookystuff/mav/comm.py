import os
import sys

import dronekit

class Endpoint(object):

    # TODO: use **local() to reduce boilerplate copies
    def __init__(self, connStrs, vehicleClass=None):
        self.connStrs = connStrs
        self.vehicleClass = vehicleClass

    @property
    def connStr(self):
        return self.connStrs[0]

# class ProxyFactory(object):
#
#     def __init__(self, ports=range(12014,12108), gcsMapping=None, polling=False):
#         # type: (list, dict, boolean) -> None
#         if gcsMapping is None:
#             gcsMapping = {'.*': ['127.0.0.1:14550']}
#
#         self.ports = ports
#         self.gcsMapping = gcsMapping
#         self.polling = polling
#
#     def nextPort(self):  # NOT static! don't move anywhere
#         port = utils.nextUnused(Proxy.usedPort, self.ports)
#         return port
#
#     def nextProxy(self, connStr, vType=None):
#         port = self.nextPort()
#         outs = None
#
#         for k in self.gcsMapping:
#             if re.match(k, connStr):
#                 outs = self.gcsMapping[k]
#
#         if not vType:
#             name = connStr
#         else:
#             name = vType + ":" + connStr
#
#         if outs:
#             try:
#                 proxy = Proxy(
#                     connStr,
#                     name,
#                     port,
#                     outs
#                 )
#                 return proxy
#             except Exception:
#                 if port in Proxy.usedPort:
#                     Proxy.usedPort.remove(port)
#                 raise
#         else:
#             return None

# how to handle interim daemon?
class Proxy(object):

    def __init__(self, connStr, name, port, outs=list(), pid=None):
        self.connStr = connStr
        self.port = port
        self.outs = outs
        self.name = name
        self.pid = pid

    @property
    def uri(self):
        return 'localhost:' + str(self.port)

    @property
    def effectiveOuts(self):
        return self.outs + [self.uri]

    def launch(self):
        if not self.pid:
            spawn = Proxy._launch(aircraft=self.name, master=self.connStr, outs=self.effectiveOuts)
            self.pid = spawn.pid

    @staticmethod
    def _launch(aircraft, setup=False, master='tcp:127.0.0.1:5760', outs={'127.0.0.1:14550'},
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
        import pexpect # included by transitive dependency
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

    def close(self):
        if self.pid:
            os.killpg(self.pid, 2)


# if all endpoints are not available, sleep for x seconds and retry.
class MAVConnection(object):
    # process local
    existing = None  # type: MAVConnection

    @staticmethod
    def getOrCreate(endpoint, proxy=None):
        # type: (Endpoint, Proxy) -> MAVConnection
        if not MAVConnection.existing:
            MAVConnection.existing = MAVConnection(endpoint, proxy)
        return MAVConnection.existing

    def __init__(self, endpoint, proxy=None):
        # type: (Endpoint, Proxy) -> None
        self.endpoint = endpoint
        self.proxy = proxy
        if self.proxy:
            self.proxy.launch()

        vehicle = dronekit.connect(
            ip=endpoint.connStr,
            wait_ready=True
        )

        self.vehicle = vehicle

    def close(self):
        self.vehicle.close()
        if self.proxy:
            self.proxy.close()

    # only for unit test.
    # takes no parameter, always move drone to a random point and yield a location after moving for 100m.
    def testMove(self):
        point =
        vehicle = self.vehicle
        # NOTE these are *very inappropriate settings*
        # to make on a real vehicle. They are leveraged
        # exclusively for simulation. Take heed!!!
        vehicle.parameters['FS_GCS_ENABLE'] = 0
        vehicle.parameters['FS_EKF_THRESH'] = 100
        print("Allowing time for parameter write")
        assureInTheAir(20, vehicle)
        print("Going to point...")
        vehicle.simple_goto(point)

        def getDistSq():
            north = vehicle.location.local_frame.north
            east = vehicle.location.local_frame.east
            return north * north + east * east

        distSq = getDistSq()
        while distSq <= 400:  # 20m
            print("moving ... " + str(sqrt(distSq)) + "m")
            distSq = getDistSq()
            time.sleep(1)

def randomLocation():
    return LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 20)

def randomLocations():
    points = map(
        lambda i: APMSimFixture.randomLocation(),
        range(0, numCores)
    )
    return points