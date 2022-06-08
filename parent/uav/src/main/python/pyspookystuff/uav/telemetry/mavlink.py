from __future__ import print_function

import logging
import math
import os
import pkgutil
import random
import runpy
import sys

import dronekit
from MAVProxy import mavproxy

from pyspookystuff.uav import VehicleFunctions
from pyspookystuff.uav.const import *
from pyspookystuff.uav.utils import retry
"""
crash course on MAVProxy:
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


class Daemon(object):

    def start(self):
        print("starting", self.fullName, str(type(self)))
        try:
            self._start()
        except:
            self.stop()
            raise

    def _start(self):
        pass

    @retry(5)
    def stop(self):
        print("stopping", self.fullName, ":", str(type(self)))
        self._stop()

    def _stop(self):
        pass

    def restart(self):
        self.stop()
        self.start()

    def __del__(self):
        try:
            self.stop()
        except:
            print(self.fullName, "!!! FAIL TO CLEAN UP !!!")
            raise

    def logPrint(self, *args):
        print(self.fullName, *args)

    @property
    def fullName(self):
        return ""


def randomLocalLocation():
    dir = random.uniform(0, 2 * math.pi)
    return dronekit.LocationLocal(50 * math.cos(dir), 50 * math.sin(dir), -10)


class Endpoint(Daemon, VehicleFunctions):
    # TODO: use **local() to reduce boilerplate copies
    def __init__(self, uri, baudRate, groundSSID, frame=None, name=""):
        # type: (str, int, int, str, str) -> None
        super(Endpoint, self).__init__(None)
        self.uri = uri

        self.baudRate = baudRate
        self.ssid = groundSSID
        self.frame = frame
        self.name = name

    @property
    def fullName(self):
        return self.name + "@" + self.uri

    @property
    def isConnected(self):
        return not (self.vehicle == None)

    @retry(daemonStartRetries)
    def _start(self):
        if not self.vehicle:
            try:
                self.vehicle = dronekit.connect(self.uri, wait_ready=True, source_system=self.ssid, baud=self.baudRate)
            except:
                print("ERROR: ENDPOINT CANNOT CONNECT TO", self.uri)
                raise

            # self.vehicle.commands.download()  # get home_location asynchronously
            # self.vehicle.wait_ready()
            return self.vehicle

    # this doesn't terminate the proxy so GCS can still see it.
    # almost useless, once interpreter is killed the connection is gone.
    def _stop(self):
        if self.vehicle:
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

        self.assureClearanceAlt(height)

        self.localOrigin = self.vehicle.location.global_relative_frame
        self.move(target)

        last_location = self.vehicle.location

        return last_location.local_frame.north, last_location.local_frame.east

    def reconnect(self):
        self.restart()


defaultProxyOptions = ('--state-basedir=temp', '--daemon', '--default-modules="link"')  # --cmd="module unload console"'


class MAVProxy(object):

    def __init__(self, master, outs, baudRate, ssid, name):
        # type: (str, list[str], int, int, str) -> None
        self.master = master
        self.outs = outs
        self.baudRate = baudRate
        self.ssid = ssid
        self.name = name

    # defaultOptions = '--daemon --cmd="module unload console"'
    def startAndBlock(self, setup=False, extraOptions=defaultProxyOptions):
        # type: (bool, tuple[str]) -> None
        # once started the only way to terminate is ending the process
        # TODO: MAVProxy can only be run in main thread! this function will block indefinitely, but there is no other options

        _argv = list(['', '--master=%s' % self.master])
        for out in self.outs:
            _argv.append('--out=%s' % out)
        if setup:
            _argv.append('--setup')
        if self.baudRate:
            _argv.append('--baudrate=%s' % self.baudRate)
        if self.ssid:
            _argv.append('--source-system=%s' % self.ssid)
        _argv.append('--aircraft=%s' % self.name)
        if extraOptions is not None:
            _argv.extend(list(extraOptions))

        print(_argv)

        oldArgv = sys.argv
        sys.argv = _argv
        try:
            # loader = pkgutil.find_loader(mavproxy.__name__)
            # fileName = loader.get_filename(mavproxy.__name__)
            # MAVPROXY = os.getenv('MAVPROXY_CMD', fileName)

            runpy.run_module(mavproxy.__name__, {"dummy": "DUMMY!"}, "__main__")  # TODO: remove experimental code

        finally:
            sys.argv = oldArgv

        # if not self.p:
        #     self._spawnProxy()
        #
        #     time.sleep(1)  # wait for process creation
        #
        #     # @retry(daemonStartRetries)
        #     def sanityCheck():
        #         try:
        #             def isAlive(i):
        #                 return self.isAlive
        #             utils.waitFor(isAlive, 10)
        #
        #             # ensure that proxy is usable, otherwise its garbage
        #             vehicle = dronekit.connect(
        #                 self.outs[0],
        #                 wait_ready=True, #  TODO change to False once stabilized
        #                 source_system=self.ssid, #  TODO: how to handle this?
        #                 baud=self.baudRate
        #             )
        #             @retry(5)
        #             def _close():
        #                 vehicle.close()
        #             _close()
        #
        #             time.sleep(2) #  wait for port to be released
        #         except:
        #             print("ERROR: PROXY CANNOT CONNECT TO", self.outs[0])
        #             self.stop()
        #             raise
        #
        #     sanityCheck()!
        #
        #     self.logPrint("Proxy spawned: PID =", self.pid, "URI =", self.outs[0])


class LocationGlobal(dronekit.LocationGlobal):

    def __init__(self, lat, lon, alt=None):
        super(LocationGlobal, self).__init__(lat, lon, alt)


class LocationGlobalRelative(dronekit.LocationGlobalRelative):

    def __init__(self, lat, lon, alt=None):
        super(LocationGlobalRelative, self).__init__(lat, lon, alt)


class LocationLocal(dronekit.LocationLocal):

    def __init__(self, north, east, down):
        super(LocationLocal, self).__init__(north, east, down)
