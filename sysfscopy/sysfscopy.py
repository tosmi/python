""":Synopsis: copy a given sys tree without following links. Links
are actually converted to simple directories or file.

**Source Code:** `sysfscopy.py`

---------------

This is the :mod:`sysfscopy` module.
"""
class SysfsCopy(object):
    def init(self):
        pass

def sysfscopy(source=None, destination=None):
    return True

if __name__ == "__main__":
    sysfscopy('/sys/class/net', '/tmp')
