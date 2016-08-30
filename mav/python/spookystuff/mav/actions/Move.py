import json

import time
from dronekit import connect, VehicleMode, LocationGlobalRelative
from nose.tools import assert_equals

class DroneAction(object): # TODO: stub

    def __init__(self, actionJSON):
        None

class Move(DroneAction):

    move = None

    def __init__(self, actionJSON):
        # type: (str, str) -> object
        self.move = json.loads(actionJSON)

    def exe(sessionJSON):
        # type: (str) -> object
        session = json.loads(sessionJSON)

        session.
