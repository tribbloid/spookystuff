### DON'T DELETE, FIXTURES!

import sys
import time

sys.path.append('/home/peng/.spookystuff/pythonpath')
import os
import simplejson as json

import pyspookystuff.mav.sim
import pyspookystuff.mav.telemetry

aPMSim60043357380016504=pyspookystuff.mav.sim.APMSim(
    iNum=json.loads(
        """
        3
        """
    )
)

proxy=pyspookystuff.mav.telemetry.Proxy(
    master="tcp:localhost:5790",
    name="DRONE",
    outs = ["udp:localhost:12015", "udp:localhost:14550"]
)

proxy.start()


### History ###

import sys
sys.path.append('/home/peng/.spookystuff/pythonpath')
import simplejson as json
import pyspookystuff.mav.telemetry
endpoint1441794875322176137=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
    """
    [ "udp:localhost:12015" ]
    """
))
link7452322945051954858=pyspookystuff.mav.telemetry.Link(endpoint=endpoint1441794875322176137)
start6307625525549872737=link7452322945051954858.start()

_temp1798473547518563724=None
_temp1798473547518563724=start6307625525549872737
print('*!?execution result!?*')
if _temp1798473547518563724:
    print(_temp1798473547518563724)
else:
    print('*!?no returned value!?*')

del(_temp1798473547518563724)
# Error interpreting:
link7452322945051954858.testMove()

link7452322945051954858.testMove()

link7452322945051954858.testMove()