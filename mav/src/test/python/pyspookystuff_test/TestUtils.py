from __future__ import print_function

import os
from unittest import TestCase

from pyspookystuff.utils import retry


class TestUtils(TestCase):
    def testRetry(self):
        """should throw the same exception"""

        @retry(3)
        def alwaysError():
            print(1/0)

        try:
            alwaysError()
        except:
            return

        raise os.error("impossible")
