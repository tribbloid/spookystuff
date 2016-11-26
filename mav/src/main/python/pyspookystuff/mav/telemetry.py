from __future__ import print_function

import json
import logging

from pyspookystuff.utils import retry

import os
import random
import sys
from math import sqrt

import dronekit
import time

from pyspookystuff.mav import assureInTheAir, noTimeout, Const

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
    def __init__(self, connStrs, vehicleClass=None):
        self.connStrs = connStrs
        self.vehicleClass = vehicleClass

    @property
    def connStr(self):
        return self.connStrs[0]


# how to handle interim daemon?
class Proxy(object):
    def __init__(self, master, name, outs=list()):
        self.master = master
        self.name = name
        self.outs = outs


def _launchProxy(aircraft, setup, master, outs, options=None, logfile=sys.stdout):
    # type: (str, bool, str, list, str, str) -> pexpect.spawn

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
    # spawn daemon?
    spawn = pexpect.spawn(cmd, logfile=logfile, timeout=60)
    spawn.delaybeforesend = 0
    return spawn


proxySpawn = None
class MayNeedProxy(object):

    def __init__(self, proxyOpt=None, proxyPIDOpt=None):
        # type: (Proxy, int) -> None
        self.proxy = proxyOpt
        self.proxyPID = proxyPIDOpt

    def _getProxy(self):
        # type: () -> int
        global proxySpawn
        if self.proxy and (not self.proxyPID): # set but not launched
            print("Proxy spawning:", json.dumps(self.proxy, default=lambda c: c.__dict__))
            proxySpawn = _launchProxy(
                aircraft=self.proxy.name,
                setup=False,
                master=self.proxy.master,
                outs=self.proxy.outs
            )
            time.sleep(1) # wait for proxy to initialize
            assert(proxySpawn.isalive())
            self.proxyPID = proxySpawn.pid
        return self.proxyPID

    def _killProxy(self):
        # type: () -> None
        if self.proxyPID:
            try:
                self.killProxy(self.proxyPID)
            except OSError:
                pass
            self.proxyPID = None

    @staticmethod
    def killProxy(pid):
        os.killpg(pid, 2)

    # may refresh proxyUID, make sure scala object is synchronized every time its called
    def _relaunchProxy(self):
        self._killProxy()
        self._getProxy()


# if all endpoints are not available, sleep for x seconds and retry.
class Link(MayNeedProxy):
    # process local
    # existing = None  # type: # DroneCommunication

    # @staticmethod
    # def getOrCreate(endpoint, proxy=None):
    #     # type: (Endpoint, Proxy) -> DroneCommunication
    #     if not DroneCommunication.existing:
    #         DroneCommunication.existing = DroneCommunication(endpoint, proxy)
    #     return DroneCommunication.existing

    def __init__(self, endpoint, proxyOpt=None, proxyPIDOpt=None):
        # type: (Endpoint, Proxy, int) -> None
        super(Link, self).__init__(proxyOpt, proxyPIDOpt)
        self.endpoint = endpoint

        # test if the endpoint really exist, if not there is no point doing the rest of it.
        # vehicle = _retryConnect(self.endpoint.connStr)

        if self.proxy:
            self.uri = self.proxy.outs[0]
        else:
            self.uri = self.endpoint.connStr

        self.vehicle = self._connectWProxy()

    @retry(Const.proxyRetries)
    def _connectWProxy(self):

        @retry(Const.connectionRetries)
        def connect():
            print("Drone connecting:", self.uri)
            vehicle = dronekit.connect(
                self.uri,
                wait_ready=True
            )
            return vehicle

        if self.proxy:
            # type: () -> dronekit.Vehicle
            try:
                self._getProxy()
                return connect()
            except:
                # proxy is broken and should be relaunched.
                self._relaunchProxy()
                raise
        else:
            return connect()

    # this doesn't terminate the proxy so GCS can still see it.
    # almost useless, once interpreter is killed the connection is gone.
    def disconnect(self):
        print("Drone disconnecting:", self.uri)
        self.vehicle.close()

    # only for unit test.
    # takes no parameter, always move drone to a random point and yield a location after moving for 100m.
    # then mark the location and instruct to attitude hold.
    def testMove(self):
        point = randomLocation()
        vehicle = self.vehicle
        # NOTE these are *very inappropriate settings*
        # to make on a real vehicle. They are leveraged
        # exclusively for simulation. Take heed!!!
        vehicle.parameters['FS_GCS_ENABLE'] = 0
        vehicle.parameters['FS_EKF_THRESH'] = 100

        assureInTheAir(20, vehicle)
        print("Going to point...")
        vehicle.simple_goto(point)

        northRef = vehicle.location.local_frame.north
        eastRef = vehicle.location.local_frame.east

        print("starting at " + str(northRef) + ":" + str(eastRef))

        def getDistSq():
            north = vehicle.location.local_frame.north - northRef
            east = vehicle.location.local_frame.east - eastRef
            print("current position " + str(north) + ":" + str(east))
            return north * north + east * east

        distSq = getDistSq()
        while distSq <= 10000:  # 20m
            noTimeout(vehicle)
            distSq = getDistSq()
            print("moving ... " + str(sqrt(distSq)) + "m")
            time.sleep(1)

        last_location = vehicle.location
        # TODO change mode: GUIDED -> POSITION_HOLD?
        vehicle.simple_goto(last_location.global_relative_frame)

        return last_location.local_frame.north, last_location.local_frame.east


def randomLocation():
    return dronekit.LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 20)