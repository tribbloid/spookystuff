from __future__ import print_function

import os

import sys
from dronekit import connect
from dronekit_sitl import SITL

sitls = []
mavproxies = []
sitl_args = ['--model', 'quad', '--home=-35.363261,149.165230,584,353']


if 'SITL_SPEEDUP' in os.environ:
    sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
if 'SITL_RATE' in os.environ:
    sitl_args += ['-r', str(os.environ['SITL_RATE'])]


def setupMAVProxy(atype, aircraft=None, setup=False, master='tcp:127.0.0.1:5760', outs={'127.0.0.1:14550'},
                  options=None, logfile=sys.stdout):
    '''launch mavproxy connected to a SIL instance'''
    import pexpect
    MAVPROXY = os.getenv('MAVPROXY_CMD', 'mavproxy.py')
    cmd = MAVPROXY + ' --master=%s' % master
    for out in outs:
        cmd += ' --out=%s' % out
    if setup:
        cmd += ' --setup'
    if aircraft is None:
        aircraft = 'test.%s' % atype
    cmd += ' --aircraft=%s' % aircraft
    if options is not None:
        cmd += ' ' + options
    ret = pexpect.spawn(cmd, logfile=logfile, timeout=60)
    ret.delaybeforesend = 0
    mavproxies.append(ret)
    return ret


def setupSITL(instance=0):
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



def teardownMAVProxy():

    global mavproxies
    for m in mavproxies :
        os.killpg(m.pid, 2)
    mavproxies = []


def teardownSITL():
    global sitls
    for sitl in sitls :
        sitl.stop()
    sitls = []