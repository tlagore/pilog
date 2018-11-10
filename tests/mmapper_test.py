import os
from ..pilogger.pilogger import Mmapper

def func(x):
    return x + 1

def test_answer():
    assert func(3) == 4


def test_open():
    fileName = os.path.join(os.path.expanduser("~"), "logs", "pilog_logs/")
    map = Mmapper(fileName)
    map.open()