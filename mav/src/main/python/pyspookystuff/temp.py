
import json

import pyspookystuff.mav.comm
endpoint8558839845084712229=pyspookystuff.mav.comm.Endpoint(
    connStrs=json.loads(
        """
        [ "tcp:127.0.0.1:5830" ]
        """
    )
)
import pyspookystuff.mav.comm
proxy4421571800750034307=pyspookystuff.mav.comm.Proxy(
    connStr=json.loads(
        """
        "tcp:127.0.0.1:5830"
        """
    ),name=json.loads(
        """
        "DRONE@tcp:127.0.0.1:5830"
        """
    ),port=json.loads(
        """
        12014
        """
    ),outs=json.loads(
        """
        [ ]
        """
    )
)
import pyspookystuff.mav.comm
connection8833937162425720024=pyspookystuff.mav.comm.MAVConnection(
    endpoint=endpoint8558839845084712229,proxy=proxy4421571800750034307
)
vehicle7595132989461308857=connection8833937162425720024.vehicle
_temp6544983804617871432=None
_temp6544983804617871432=vehicle7595132989461308857
print('*!?execution result!?*')
if _temp6544983804617871432:
    print(_temp6544983804617871432)
else:
    print('*!?no returned value!?*')
del(_temp6544983804617871432)