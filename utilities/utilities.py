from datetime import datetime
import sys

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def time_message(message):
    return datetime.now().strftime("!! %H:%M:%S - ") + "{0}".format(message)

def tprint(message):
    print(time_message(message))