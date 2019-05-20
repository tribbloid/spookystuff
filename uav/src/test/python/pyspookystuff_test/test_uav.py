# tests in this package are only tested locally but not on CI server, they should be lightweight.

from __future__ import print_function

import os
from unittest import TestCase

from pyspookystuff.uav.utils import retry


class TestUtils(TestCase):

    def test_retryCanFail(self):
        """should throw the same exception"""
        # TODO: test counter

        @retry(3)
        def alwaysError():
            print(1 / 0)

        try:
            alwaysError()
        except:
            return

        raise os.error("impossible")
