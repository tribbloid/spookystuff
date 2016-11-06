"""
Crash course on MAVProxy
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

import os
import random
import sys
from math import sqrt

import dronekit
import time

from pyspookystuff.mav import assureInTheAir, noTimeout


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
    def __init__(self, master, name, outs=list(), pid=None):
        self.master = master
        self.outs = outs
        self.name = name
        self.pid = pid

        self.spawn = None

    def launch(self):
        if not self.pid:
            spawn = Proxy._launch(
                aircraft=self.name,
                setup=False,
                master=self.master,
                outs=self.outs
            )
            self.pid = spawn.pid
            self.spawn = spawn

    @staticmethod
    def _launch(aircraft, setup, master, outs, options=None, logfile=sys.stdout):
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
        ret = pexpect.spawn(cmd, logfile=logfile, timeout=60)
        ret.delaybeforesend = 0
        return ret

    def close(self):
        if self.pid:
            os.killpg(self.pid, 2)


# if all endpoints are not available, sleep for x seconds and retry.
class DroneCommunication(object):
    # process local
    existing = None  # type: DroneCommunication

    @staticmethod
    def getOrCreate(endpoint, proxy=None):
        # type: (Endpoint, Proxy) -> DroneCommunication
        if not DroneCommunication.existing:
            DroneCommunication.existing = DroneCommunication(endpoint, proxy)
        return DroneCommunication.existing

    def __init__(self, endpoint, proxy=None):
        # type: (Endpoint, Proxy) -> None
        self.endpoint = endpoint
        self.proxy = proxy

        # test if the endpoint really exist, if not there is no point doing the rest of it.
        # vehicle = _retryConnect(self.endpoint.connStr)

        if self.proxy:
            self.proxy.launch()
            self.uri = self.proxy.outs[0]

        else:
            self.uri = self.endpoint.connStr

        self._vehicle = None

    @property
    def vehicle(self):
        if not self._vehicle:
            self._vehicle = _retryConnect(self.uri)
        return self._vehicle

    def close(self):
        if self._vehicle:
            self._vehicle.close()
        if self.proxy:
            self.proxy.close()

    # only for unit test.
    # takes no parameter, always move drone to a random point and yield a location after moving for 100m.
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
        while distSq <= 400:  # 20m
            noTimeout(vehicle)
            distSq = getDistSq()
            print("moving ... " + str(sqrt(distSq)) + "m")
            time.sleep(1)

        return vehicle.location.local_frame.north, vehicle.location.local_frame.east


def randomLocation():
    return dronekit.LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 20)

def _retryConnect(uri, maxTrial=3):
    for i in range(0, 100):
        try:
            print "Trial ", str(i), ": connecting to ", str(uri)
            vehicle = dronekit.connect(
                uri,
                wait_ready=True
            )
            return vehicle
        except dronekit.APIException as e:
            print(e)
            if i >= maxTrial:
                raise
            else:
                continue
