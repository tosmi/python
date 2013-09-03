"""
:Synopsis: test sysfscopy

**Source Code:** `test_sysfscopy.py`

---------------

This is the :mod:`test_sysfscopy` module.


"""
import unittest
import sysfscopy

class TestSysfsCopy(unittest.TestCase):
    def test_sysfscopy(self,):
        self.assertTrue(sysfscopy.sysfscopy('/sys/class/net', '/tmp'))

def suite():
    tests = [ 'test_sysfscopy', ]
    suite = unittest.TestSuite( map(TestSysfsCopy, tests) )
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner( verbosity=2).run( suite() )
