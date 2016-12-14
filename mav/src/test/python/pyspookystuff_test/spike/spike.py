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
        1
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
import os
import simplejson as json
import pyspookystuff.mav.telemetry
endpoint8044150411413521834=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
    """
    [ "tcp:localhost:5770" ]
    """
))
link488544169442936378=pyspookystuff.mav.telemetry.Link(endpoint=endpoint8044150411413521834)
start2640234324586639149=link488544169442936378.start()

### Error interpreting: ###

_temp300130972983225840=None
_temp300130972983225840=start2640234324586639149
print('*!?execution result!?*')
if _temp300130972983225840:
    print(_temp300130972983225840)
else:
    print('*!?no returned value!?*')
del(_temp300130972983225840)