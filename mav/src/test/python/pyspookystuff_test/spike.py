### DON'T DELETE, FIXTURES!

import sys
sys.path.append('/home/peng/.spookystuff/pythonpath')
import os
import simplejson as json

import pyspookystuff.mav.sim
aPMSim60043357380016504=pyspookystuff.mav.sim.APMSim(
    iNum=json.loads(
        """
        0
        """
    )
)


# Error interpreting:
import sys
sys.path.append('/home/peng/.spookystuff/pythonpath')
import os
import simplejson as json
import pyspookystuff.mav.telemetry
endpoint4745652708396534440=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
    """
    [ "tcp:localhost:5760" ]
    """
))
proxy6998002939612108927=pyspookystuff.mav.telemetry.Proxy(master=json.loads(
    """
    "tcp:localhost:5760"
    """
), name=json.loads(
    """
    "DRONE"
    """
), outs=json.loads(
    """
    [ "udp:localhost:12022", "udp:localhost:14550" ]
    """
))
droneCommunication2093055258224904998=pyspookystuff.mav.telemetry.Link(endpoint=endpoint4745652708396534440, proxy=proxy6998002939612108927)
testMove7484731509822679384=droneCommunication2093055258224904998.testMove()