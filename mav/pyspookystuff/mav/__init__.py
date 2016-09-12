from __future__ import print_function

import os

import sys
from dronekit import connect
from dronekit_sitl import SITL

from pyspookystuff import PyspookyException

sitls = []
mavproxies = []
sitl_args = ['--model', 'quad', '--home=-35.363261,149.165230,584,353']


if 'SITL_SPEEDUP' in os.environ:
    sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
if 'SITL_RATE' in os.environ:
    sitl_args += ['-r', str(os.environ['SITL_RATE'])]


# def getOrCreateMAVProxy(routing):

class DronePoolDepletedException(PyspookyException):
    pass


class PortDepletedException(PyspookyException):
    pass





def proxyUp(aircraft, setup=False, master='tcp:127.0.0.1:5760', outs={'127.0.0.1:14550'},
            options=None, logfile=sys.stdout):
    """
    launch mavproxy connected to a SIL instance
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
    mavproxies.append(ret)
    return ret


def proxyDown():

    global mavproxies
    for m in mavproxies :
        os.killpg(m.pid, 2)
    mavproxies = []


def sitlUp(instance=0):
    global sitls
    args = sitl_args + ['-I' + str(instance)]
    sitl = SITL()
    sitl.download('copter', '3.3')

    sitl.launch(args, await_ready=True, restart=True)

    key = 'SYSID_THISMAV'
    value = instance + 1

    setParamAndRelaunch(sitl, args, tcp_master(instance), key, value)

    sitls.append(sitl)


def tcp_master(instance):
    return 'tcp:127.0.0.1:' + str(5760 + instance*10)


def setParamAndRelaunch(sitl, args, connString, key, value):

    wd = sitl.wd
    v = connect(connString, wait_ready=True)
    v.parameters.set(key, value, wait_ready=True)
    v.close()
    sitl.stop()
    sitl.launch(args, await_ready=True, restart=True, wd=wd, use_saved_data=True)
    v = connect(connString, wait_ready=True)
    # This fn actually rate limits itself to every 2s.
    # Just retry with persistence to get our first param stream.
    v._master.param_fetch_all()
    v.wait_ready()
    actualValue = v._params_map[key]
    assert actualValue == value
    v.close()


def sitlDown():
    global sitls
    for sitl in sitls :
        sitl.stop()
    sitls = []