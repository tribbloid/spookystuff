from pyspookystuff.mav.actions import DummyPyAction
from pyspookystuff_test.mav import ParallelCase


def run(i):
    a = DummyPyAction({"a": {"value": i}})
    result = a.dummy(2, 3)
    print(result)
    return result

class DryRun(ParallelCase):

    @staticmethod
    def testFunctions():
        return [run]
