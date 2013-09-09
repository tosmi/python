"""
:Synopsis: test networkinfo

**Source Code:** `test_networkinfo.py`

---------------

This is the :mod:`test_networkinfo` module.


"""
import unittest
import networkinfo

class TestNetworkInfo(unittest.TestCase):
    def test_networkinfo_init(self,):
        ni = networkinfo.NetworkInfo('/tmp/test')
        self.assertTrue( ni )

    def test_networkinfo_collect(self,):
        ni = networkinfo.NetworkInfo('/tmp/test')
        ni.collect()

def suite():
    tests = [ 'test_networkinfo_init',
              'test_networkinfo_collect',
    ]
    suite = unittest.TestSuite( map(TestNetworkInfo, tests) )
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner( verbosity=2).run( suite() )
