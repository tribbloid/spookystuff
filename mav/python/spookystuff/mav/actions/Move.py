import json

import time
from dronekit import connect, VehicleMode, LocationGlobalRelative
from nose.tools import assert_equals

class Move(object):

    this = None

    def __init__(self, actionJSON):
        # type: (str, str) -> object
        self.this = json.loads(actionJSON)

    def exe(sessionJSON):
        # type: (str) -> object
        session = json.loads(sessionJSON)

        session.
