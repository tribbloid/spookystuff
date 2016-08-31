from __future__ import print_function
import os
import tempfile

import sys
from dronekit import connect

from dronekit_sitl import SITL
from nose.tools import assert_equals, with_setup
import time

sitls = []
mavproxies = []
sitl_args = ['--model', 'quad', '--home=-35.363261,149.165230,584,353']

if 'SITL_SPEEDUP' in os.environ:
    sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
if 'SITL_RATE' in os.environ:
    sitl_args += ['-r', str(os.environ['SITL_RATE'])]

def tcp_master(instance):
    return 'tcp:127.0.0.1:' + str(5760 + instance*10)

def setup_sitl(instance=0):
    global sitls
    args = sitl_args + ['-I' + str(instance)]
    sitl = SITL()
    sitl.download('copter', '3.3')

    sitl.launch(args, await_ready=True, restart=True)

    key = 'SYSID_THISMAV'
    value = instance + 1

    setParamAndRelaunch(sitl, args, tcp_master(instance), key, value)

    sitls.append(sitl)

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

def setup_sitl_mavproxy(atype='quad', options=None, instance=0):
    global mavproxies
    setup_sitl(instance)

    time.sleep(2)
    # out = '127.0.0.1:' + str(14550 + instance*10)
    out = '127.0.0.1:' + str(14550)
    mavproxy = start_MAVProxy_SIL(atype=atype + str(instance), master=tcp_master(instance), out=out, options=options)
    mavproxies.append(mavproxy)

def start_MAVProxy_SIL(atype, aircraft=None, setup=False, master='tcp:127.0.0.1:5760', out='127.0.0.1:14550',
                       options=None, logfile=sys.stdout):
    '''launch mavproxy connected to a SIL instance'''
    import pexpect
    global close_list
    MAVPROXY = os.getenv('MAVPROXY_CMD', 'mavproxy.py')
    cmd = MAVPROXY + ' --master=%s' % master + ' --out=%s' % out
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

    global sitls, mavproxies
    for m in mavproxies :
        os.killpg(m.pid, 2)
    mavproxies = []

    for sitl in sitls :
        sitl.stop()
    sitls = []

def with_sitl(fn):

    @with_setup(setup_sitl, teardown_sitl)
    def test(*args, **kargs):
        tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
        teardown_sitl()
        setup_sitl_mavproxy()
        udp = fn('127.0.0.1:14550', *args, **kargs)
        assert_equals(tcp, udp)
        return tcp

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

def with_sitl_3way(fn):

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
