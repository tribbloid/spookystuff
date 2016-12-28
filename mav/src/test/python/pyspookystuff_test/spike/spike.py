import dronekit
import pexpect
import sys
import time
from dronekit_sitl import SITL

sys.path.append('/home/peng/.spookystuff/pythonpath')
import os

sitl_args = ['--model', 'quad', '--home=-35.363261,149.165230,584,353']
# if 'SITL_SPEEDUP' in os.environ:
#     sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
# if 'SITL_RATE' in os.environ:
#     sitl_args += ['-r', str(os.environ['SITL_RATE'])]

sitl = SITL()
sitl.download('copter', '3.3')
sitl.launch(sitl_args, await_ready=True, restart=True)

# for i in range(1, 20):
#     vehicle = dronekit.connect(
#         "tcp:localhost:5760",
#         wait_ready=True
#     )
#     vehicle.commands.download()
#     vehicle.commands.wait_ready()
#     vehicle.close()

def spawnProxy(aircraft, setup, master, outs,
               options='', logfile=sys.stdout):
    # type: (str, bool, str, list, str, str) -> object

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

p = spawnProxy(
    aircraft="DRONE",
    setup=False,
    master="tcp:localhost:5760",
    outs=["udp:localhost:12052", "udp:localhost:14550"]
)

time.sleep(1) # wait for proxy to initialize

for i in range(1, 20):
    vehicle = dronekit.connect(
        "udp:localhost:12052",
        wait_ready=True
    )
    vehicle.commands.download()
    vehicle.commands.wait_ready()
    vehicle.close()