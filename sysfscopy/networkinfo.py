
"""
:Synopsis: Base class to store information about network interfaces.

**Source Code:** `networkinfo.py`

---------------

This is the :mod:`networkinfo` module. T


"""
import os

import networkinterface

class NetworkInfo(object):
    """collect various information about the connected network interfaces.

    """
    def __init__(self, sysfs_path='/sys'):
        self._networkinterfaces = []
        self._sysfs_path = sysfs_path

    def collect(self,):
        interface_names = [ i for i in self._get_interface_names() ]
        for i in interface_names:
            print i

    def _get_interface_names(self,):
        for name in os.listdir(self._sysfs_path):
            yield name
