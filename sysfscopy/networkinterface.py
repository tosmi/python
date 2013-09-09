
"""
:Synopsis: Base class to store information about network interfaces.

**Source Code:** `networkinterface.py`

---------------

This is the :mod:`networkinterface` module.

"""
class NetworkInterface(object):
    def __init__(self,):
        """Attributes:
        `name`: the name if this interface. e.g. eth0
        `mtu`: the maximum transmission unit size for this interface
        `ipaddresses`: array of all ip addresses configured on this interface
        `dnsname`: for dns names for all ip addresses
        `mtu`: the maximum transmission unit size
        `speed`: interface speed
        `duplex`: full or half duplex
        `is_physical`: is this a physical or virtual interface
        `has_link`: is this interface connected
        """
        self.name = name
        self.macaddr= macaddr
        self.ipaddresses = ipaddresses
        self.dnsnames = dnsnames
        self.mtu  = mtu
        self.speed = speed
        self.duplex = duplex
        self.is_physical = is_physical
        self.has_link = has_link
