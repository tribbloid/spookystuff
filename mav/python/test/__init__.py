from __future__ import print_function
import os
import sys

from dronekit_sitl import SITL
# from dronekit_sitl.pysim import util
from nose.tools import assert_equals, with_setup
import time

sitl = None
mavproxy = None
sitl_args = ['-I0', '--model', 'quad', '--home=-35.363261,149.165230,584,353']

if 'SITL_SPEEDUP' in os.environ:
    sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
if 'SITL_RATE' in os.environ:
    sitl_args += ['-r', str(os.environ['SITL_RATE'])]

def setup_sitl():
    global sitl
    sitl = SITL()
    sitl.download('copter', '3.3')
    sitl.launch(sitl_args, await_ready=True, restart=True)

def setup_sitl_mavproxy(atype='copter', options=None):
    global mavproxy
    setup_sitl()

    time.sleep(2)
    mavproxy = start_MAVProxy_SIL(atype=atype, options=options)

def start_MAVProxy_SIL(atype, aircraft=None, setup=False, master='tcp:127.0.0.1:5760',
                       options=None, logfile=sys.stdout):
    '''launch mavproxy connected to a SIL instance'''
    import pexpect
    global close_list
    MAVPROXY = os.getenv('MAVPROXY_CMD', 'mavproxy.py')
    cmd = MAVPROXY + ' --master=%s --out=127.0.0.1:14550' % master
    if setup:
        cmd += ' --setup'
    if aircraft is None:
        aircraft = 'test.%s' % atype
    cmd += ' --aircraft=%s' % aircraft
    if options is not None:
        cmd += ' ' + options
    ret = pexpect.spawn(cmd, logfile=logfile, timeout=60)
    ret.delaybeforesend = 0
    return ret

def teardown_sitl():
    global sitl, mavproxy
    if mavproxy is not None:
        # mavproxy.sendline ('set requireexit True')
        # mavproxy.sendline ('exit')
        # time.sleep(3)
        # print('exit: exitstatus=', mavproxy.exitstatus, " signalstatus=", mavproxy.signalstatus)

        os.killpg(mavproxy.pid, 2)

        # mavproxy.close(True)
        # time.sleep(3)
        # print('close: exitstatus=', mavproxy.exitstatus, " signalstatus=", mavproxy.signalstatus)
        #
        # # if not mavproxy.terminated:
        # mavproxy.terminate(True)
        # time.sleep(3)
        # print('terminate: exitstatus=', mavproxy.exitstatus, " signalstatus=", mavproxy.signalstatus)
        # # if not mavproxy.terminated:
        # mavproxy.kill(9)
        # time.sleep(3)
        # print('kill: exitstatus=', mavproxy.exitstatus, " signalstatus=", mavproxy.signalstatus)
        # print('pid: ', mavproxy.pid)

        mavproxy=None

    sitl.stop()

def with_sitl(fn):

    @with_setup(setup_sitl, teardown_sitl)
    def test(*args, **kargs):
        tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
        teardown_sitl()
        setup_sitl_mavproxy()
        udp = fn('127.0.0.1:14550', *args, **kargs)
        assert_equals(tcp, udp)
        return tcp

    # @with_setup(setup_sitl, teardown_sitl)
    # def test_tcp(*args, **kargs):
    #     tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
    #     return tcp
    #
    # @with_setup(setup_sitl_mavproxy, teardown_sitl)
    # def test_udp(*args, **kargs):
    #     udp = fn('127.0.0.1:14550', *args, **kargs)
    #     return udp

    test.__name__ = fn.__name__
    return test

def with_sitl_tcp(fn):

    @with_setup(setup_sitl, teardown_sitl)
    def test_tcp(*args, **kargs):
        tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
        return tcp

    test_tcp.__name__ = fn.__name__
    return test_tcp

def with_sitl_udp(fn):

    @with_setup(setup_sitl_mavproxy, teardown_sitl)
    def test_udp(*args, **kargs):
        udp = fn('127.0.0.1:14550', *args, **kargs)
        return udp

    test_udp.__name__ = fn.__name__
    return test_udp

def with_sitl_multi(fn):

    @with_setup(setup_sitl_mavproxy(options='--out=127.0.0.1:10092'), teardown_sitl)
    def test_udp(*args, **kargs):
        udp = fn('127.0.0.1:10092', *args, **kargs)
        return udp

    test_udp.__name__ = fn.__name__
    return test_udp

def wait_for(condition, time_max):
    time_start = time.time()
    while not condition():
        if time.time() - time_start > time_max:
            break
        time.sleep(0.1)
