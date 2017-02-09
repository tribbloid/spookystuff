from __future__ import print_function

import logging
import math
import os
import pkgutil
import random
import sys
import time

import dronekit
import sarge
from MAVProxy import mavproxy
from pyspookystuff.uav.const import *

from pyspookystuff.uav import VehicleFunctions, utils
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
    dir = random.uniform(0,2*math.pi)
    return dronekit.LocationLocal(50*math.cos(dir), 50*math.sin(dir), -10)


class Endpoint(Daemon, VehicleFunctions):
    # TODO: use **local() to reduce boilerplate copies
    def __init__(self, uri, baudRate, ssid, frame=None, name=""):
        # type: (str, int, int, str, str) -> None
        super(Endpoint, self).__init__(None)
        self.uri = uri

        self.baudRate = baudRate
        self.ssid = ssid
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
                self.vehicle = dronekit.connect(
                    self.uri,
                    wait_ready=True,
                    source_system=self.ssid,
                    baud=self.baudRate
                )
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


defaultProxyOptions = '--state-basedir=temp --daemon --default-modules="link"'  # --cmd="module unload console"'
class Proxy(Daemon):
    def __init__(self, master, outs, baudRate, ssid, name):
        # type: (str, list[str], int, int, str) -> None
        super(Proxy, self).__init__()
        self.master = master
        self.outs = outs
        self.baudRate = baudRate
        self.ssid = ssid
        self.name = name
        self.p = None

    @property
    def process(self):
        if self.p:
            return self.p.processes[0]
        else:
            return None

    @property
    def fullName(self):
        return self.name + "@" + self.master + ">" + '/'.join(self.outs)

    # defaultOptions = '--daemon --cmd="module unload console"'
    def _spawnProxy(self, setup=False, options=defaultProxyOptions, logfile=sys.stdout):
        # type: (bool, str, str) -> None

        loader = pkgutil.find_loader(mavproxy.__name__)
        fileName = loader.get_filename(mavproxy.__name__)
        MAVPROXY = os.getenv('MAVPROXY_CMD', fileName)

        cmd = 'python ' + MAVPROXY + ' --master=%s' % self.master
        for out in self.outs:
            cmd += ' --out=%s' % out
        if setup:
            cmd += ' --setup'
        if self.baudRate:
            cmd += ' --baudrate=%s' % self.baudRate
        if self.ssid:
            cmd += ' --source-system=%s' % self.ssid
        cmd += ' --aircraft=%s' % self.name
        if options is not None:
            cmd += ' ' + options

        print(cmd)

        # TODO too heavyweight! exec in new daemon Thread + interpreter termination is good enough.
        # using solution of [http://stackoverflow.com/questions/11269575/how-to-hide-output-of-subprocess-in-python-2-7]
        pipeline = sarge.run(
            cmd, async=True,
            env={'PYTHONPATH': ':'.join(sys.path)}, stderr=utils.DEVNULL)

        self.p = pipeline

    @retry(daemonStartRetries)
    def _start(self):
        # type: () -> None

        if not self.p:
            self._spawnProxy()

            time.sleep(1)  # wait for process creation

            # @retry(daemonStartRetries)
            def sanityCheck():
                try:
                    def isAlive(i):
                        return self.isAlive
                    utils.waitFor(isAlive, 10)

                    # ensure that proxy is usable, otherwise its garbage
                    vehicle = dronekit.connect(
                        self.outs[0],
                        wait_ready=True, #  TODO change to False once stabilized
                        source_system=self.ssid, #  TODO: how to handle this?
                        baud=self.baudRate
                    )
                    @retry(5)
                    def _close():
                        vehicle.close()
                    _close()

                    time.sleep(2) #  wait for port to be released
                except:
                    print("ERROR: PROXY CANNOT CONNECT TO", self.outs[0])
                    self.stop()
                    raise

            sanityCheck()

            self.logPrint("Proxy spawned: PID =", self.pid, "URI =", self.outs[0])

    def _stop(self):
        if self.p:

            for command in self.p.commands:
                try:
                    command.terminate()
                except:
                    pass

            def isDead(i):
                return not self.isAlive
            utils.waitFor(isDead, 10)

            self.p = None

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
            return self.process.poll() is None
        else:
            return False
