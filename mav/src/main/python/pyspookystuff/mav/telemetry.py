from __future__ import print_function

import logging
import os
import random

import dronekit
import sys
import time

from pyspookystuff.mav import Const, VehicleFunctions
from pyspookystuff.mav.utils import retry

"""
Crash course on MAVProxy:
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

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'fbloggs'}

class Endpoint(object):
    # TODO: use **local() to reduce boilerplate copies
    def __init__(self, connStrs, vehicleTypeOpt=None):
        self.connStrs = connStrs
        self.vehicleType = vehicleTypeOpt

    @property
    def connStr(self):
        return self.connStrs[0]

class Daemon(object):

    def start(self):
        pass

    def stop(self):
        pass

    def restart(self):
        self.stop()
        self.start()

    def __del__(self):
        self.stop()

    def logPrint(self, *args):
        print(self.fullName, args)

    @property
    def fullName(self):
        return None


class Proxy(Daemon):

    def __init__(self, master, name, outs=list()):
        # type: (str, str, list[str]) -> None
        super(Proxy, self).__init__()
        self.master = master
        self.name = name
        self.outs = outs
        self.process = None

    @property
    def fullName(self):
        return self.name + "@" + self.master + ">" + self.outs[0]

    def start(self):
        # type: () -> int

        # self.logPrint("Proxy spawning:", json.dumps(self, default=lambda c: c.__dict__))
        if not self.process:
            p = spawnProxy(
                aircraft=self.fullName,
                setup=False,
                master=self.master,
                outs=self.outs
            )
            self.process = p

            time.sleep(1) # wait for proxy to initialize
            assert self.isAlive
            self.logPrint("Proxy spawned: PID =", self.pid)
        return self.pid

    def stop(self):
        if self.process:
            Proxy.killPID(self.process.pid)
            self.process = None

    @staticmethod
    def killPID(pid):
        try:
            os.killpg(pid, 2)
            print("Proxy killed: PID =", pid)
        except OSError:
            pass

    @property
    def pid(self):
        return self.process.pid

    @property
    def isAlive(self):
        if self.process:
            return self.process.isalive()
        else:
            return False

defaultOptions = '--state-basedir=temp --daemon --default-modules="link" --cmd="module unload console"'
# defaultOptions = '--daemon --cmd="module unload console"'

def spawnProxy(aircraft, setup, master, outs,
               options=defaultOptions, logfile=sys.stdout):
    # type: (str, bool, str, list, str, str) -> object

    import pexpect  # included by transitive dependency
    MAVPROXY = os.getenv('MAVPROXY_CMD', 'mavproxy.py')
    cmd = MAVPROXY + ' --master=%s' % master
    for out in outs:
        cmd += ' --out=%s' % out
    if setup:
        cmd += ' --setup'
    cmd += ' --aircraft=%s' % aircraft
    if options is not None:
        cmd += ' ' + options

    print(cmd)

    p = pexpect.spawn(cmd, logfile=logfile, timeout=60, ignore_sighup=True)
    p.delaybeforesend = 0

    return p


# if all endpoints are not available, sleep for x seconds and retry.
class Link(Daemon, VehicleFunctions):
    # process local
    # existing = None  # type: # DroneCommunication

    def __init__(self, endpoint, proxyOpt=None):
        # type: (Endpoint, Proxy) -> None
        super(Link, self).__init__(None)
        self.endpoint = endpoint
        self.proxy = proxyOpt

        # test if the endpoint really exist, if not there is no point doing the rest of it.
        # vehicle = _retryConnect(self.endpoint.connStr)

        if self.proxy:
            self.uri = self.proxy.outs[0]
        else:
            self.uri = self.endpoint.connStr

    @property
    def fullName(self):
        if self.proxy:
            return self.proxy.fullName
        else:
            return "DRONE@" + self.endpoint.connStr

    @property
    def isConnect(self):
        return not (self.vehicle == None)

    @retry(Const.connectionRetries)
    def start(self):
        if not self.vehicle:
            self.logPrint("Drone connecting:", self.uri)
            self.vehicle = dronekit.connect(
                self.uri,
                wait_ready=True
            )
            return self.vehicle

    # this doesn't terminate the proxy so GCS can still see it.
    # almost useless, once interpreter is killed the connection is gone.
    def stop(self):
        if self.vehicle:
            self.logPrint("Drone disconnecting:", self.uri)
            self.vehicle.close()
            self.vehicle = None

    # only for unit test.
    # takes no parameter, always move drone to a random point and yield a location after moving for 100m.
    # then mark the location and instruct to attitude hold.
    def testMove(self, height=10, dist=30):
        # type: (float, float) -> tuple[float, float]
        target = randomLocalLocation()

        # NOTE these are *very inappropriate settings*
        # to make on a real vehicle. They are leveraged
        # exclusively for simulation. Take heed!!!

        self.assureClearanceAltitude(height)

        self.localOrigin = self.vehicle.location.global_frame
        self.move(target)

        last_location = self.vehicle.location

        return last_location.local_frame.north, last_location.local_frame.east


def randomLocation():
    return dronekit.LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 10)


def randomLocalLocation():
    return dronekit.LocationLocal(random.uniform(-50, 50), random.uniform(-50, 50), -10)

