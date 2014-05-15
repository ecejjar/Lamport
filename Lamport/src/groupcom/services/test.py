'''
Created on 08/03/2013

@author: ecejjar
'''

import groupcom
from services import FileCaster
import unittest
import socket

class FileCasterTestReceiver:
    '''
    To be run in remote computer before running the unit tests below
    '''
    pass

class FileCasterTest(unittest.TestCase):

    def testCastFile(self):
        testFile = "test.py"
        port = 2000
        grp_addr = "224.0.0.1"
        host = socket.gethostbyname(socket.gethostname())
        print("Creating FileCaster on interface %s bound to %s" % (host, grp_addr))
        caster = FileCaster((host, port), (grp_addr, port), 32)
        caster.cast(testFile)
        caster.serve_forever()
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()