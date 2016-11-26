### DON'T DELETE, FIXTURES!

import sys
sys.path.append('/home/peng/.spookystuff/pythonpath')
import simplejson as json

import pyspookystuff.mav.sim
aPMSim=pyspookystuff.mav.sim.APMSim(
    iNum=json.loads(
        """
        4
        """
    )
)

# Error interpreting:
import sys
sys.path.append('/home/peng/.spookystuff/pythonpath')
import simplejson as json
import pyspookystuff.mav.telemetry

endpoint=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
    """
    [ "tcp:localhost:5800" ]
    """
))
proxy=pyspookystuff.mav.telemetry.Proxy(master=json.loads(
    """
    "tcp:localhost:5800"
    """
), outs=json.loads(
    """
    [ "udp:localhost:12015", "udp:localhost:14550" ]
    """
), name=json.loads(
    """
    "DRONE@tcp:localhost:5800"
    """
))
link=pyspookystuff.mav.telemetry.Link(endpoint=endpoint, proxyOpt=proxy)