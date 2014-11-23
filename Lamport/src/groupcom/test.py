'''
Created on 20/02/2013

@author: ecejjar
'''

from server import LogicalClockServer, McastServer, McastRouter, RMcastServer, SequencedMessage, ProtocolAgent, RepeatableTimer, StateXferAgent
from services import LeaderElection, Paxos 
from socketserver import BaseRequestHandler
from threading import  Thread, Timer, Lock
from time import sleep
from collections import deque, namedtuple
from functools import reduce, wraps
from itertools import count
import unittest
import socket
import os
import re
import logging

if not hasattr(unittest, 'skip'):
    unittest.skip = lambda func: func   # Python 3.0 and lower
    
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

class ServerTest(unittest.TestCase):

    def setUp ( self ):
        self.__msgq = deque()
        self.__abort = False
        self.__duprcvd = False
        
        # NOTE: in Linux when connected over WLAN,
        # if I let the kernel pick the IP address to use no multicast message is received
        self.__hostaddr = socket.gethostname()
        
    def handle ( self, msg, src ):
        print("%s: received from %s: %s" % (type(self).__name__, src, msg))
        self.__msgq.append(msg)

    def handleOOB ( self, msg, src ):
        print("%s: received OOB from %s: %s" % (type(self).__name__, src, msg))
        self.__msgq.append(msg)
        
    def handleException ( self, type_, data ):
        if type_ is RMcastServer.SND_EXCEPTION:
            print("%s: unable to recover previously sent message with sequence number %s" % (type(self).__name__, data))
        elif type_ is RMcastServer.RCV_EXCEPTION:
            print("%s: gave-up asking for missing message for group %s" % (type(self).__name__, data))
            self.__abort = True
        else:
            print("%s: unknown error of type %d notified by lower layer" % (type(self).__name__, type_))

    def testMcastServer ( self ):
        testData = bytes("Echo!", "utf8")
        def testfunc ( s ):
            s.send(testData)
            sleep(3)
            s.shutdown()
            
        class TestHandler(BaseRequestHandler):
            def handle ( self ):
                data = self.request[0].strip()
                socket = self.request[1]
                print("Received %s from %s" % (data, self.client_address[0]))
                assert(testData == data)
                server.result = data
                            
        port = 2000
        grp_addr = "224.0.0.1"
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating McastServer on interface %s bound to %s" % (host, grp_addr))
        server = McastServer((grp_addr, port), (host, port), TestHandler)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertEqual(testData, server.result, "Server stored result: %s" % str(server.result))
        finally:
            server.socket.close()
            
    def testMcastRouter ( self ):
        testData = "Echo!"
        port1, port2 = 2001, 2002
        grp_addr1, grp_addr2 = "224.0.0.1", "224.0.0.2"
        def testfunc ( c ):
            for seq in range(1, 6): c.send(msg(seq))
            sleep(5)
            router.shutdown()
            
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating UDP socket on interface %s:%d" % (host, 2000))
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.bind((host, 2000))
        client.connect((grp_addr1, port1))
        print("Creating McastRouter on interface %s bound to %s:%d and %s:%d" % (host, grp_addr1, port1, grp_addr2, port2))
        router = McastRouter((host, port1), (grp_addr1, port1), (host, port2), (grp_addr2, port2))
        try:
            msg = lambda s: bytes(testData + str(s), 'utf8')        
            Timer(5, testfunc, args=(client,)).start()
            router.route_forever()
        finally:
            client.close()
            router.server1.socket.close()
            router.server2.socket.close()  
        
    def testRMcastServer ( self ):
        testData = "Echo!"
        port = 2003
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')        
        def testfunc ( s ):
            for seq in range(1, 11): s.send(msg(seq))
            sleep(3)
            s.shutdown()
            
        self.__msgq.clear()
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = RMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1, 11)], "Basic RMcast server test failed")
        finally:
            server.socket.close()
            
    def testRMcastServerOrdering ( self ):
        testData = "Echo!"
        port = 2004
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')
        def testfunc ( s ):
            for seq in (1, 2, 5, 4, 3):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch, ack=s.ack, body=msg(seq))))
            sleep(3)
            s.shutdown() 

        host = socket.gethostbyname(self.__hostaddr)
        self.__msgq.clear()
        print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = RMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1, 6)], "RMcast server ordering test failed")
        finally:
            server.socket.close()
         
    def testRMcastServerDiscardDups ( self ):
        testData = "Echo!"
        port = 2005
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')
        def testfunc ( s ):
            for seq in (1, 2, 2, 1, 3):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch, ack=s.ack, body=msg(seq))))
            sleep(3)
            s.shutdown()
             
        self.__msgq.clear()
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = RMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1, 4)], "RMcast server discard duplicates test failed")
        finally:
            server.socket.close()

    def testRMcastServerNak ( self ):
        testData = "Echo!"
        port = 2006
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')
        def testfunc ( s ):
            for seq in (1, 2, 4, 5, 6, 7):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch, ack=s.ack, body=msg(seq))))
            sleep(3)
            s.shutdown()
             
        self.__msgq.clear()
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = RMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1,3)], "RMcast server NAK test failed")
            self.assertTrue(self.__abort, "RMcast server NAK test failed: didn't receive abort notification")
        finally:
            server.socket.close()
        
    def testRMcastServerRestart ( self ):
        testData = "Echo!"
        port = 2007
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')
        def testfunc ( s ):
            for seq in range(1, 4):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch, ack=s.ack, body=msg(seq))))
            sleep(3)
            for seq in range(1, 4):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch+1, ack=s.ack, body=msg(3+seq))))
            sleep(3)
            s.shutdown() 

        self.__msgq.clear()
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = RMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1,7)], "RMcast server restart test failed")
        finally:
            server.socket.close()
        
    def testRMcastServerRestartNak ( self ):
        testData = "Echo!"
        port = 2008
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')
        def testfunc ( s ):
            for seq in (1, 3):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch, ack=s.ack, body=msg(seq))))
            sleep(3)
            for seq in range(1, 4):
                McastServer.send(s, s.encode(SequencedMessage(seq=seq, epoch=s.epoch+1, ack=s.ack, body=msg(3+seq))))
            sleep(3)
            s.shutdown()
             
        self.__msgq.clear()
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = RMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertCountEqual(self.__msgq, [msg(seq) for seq in (1, 4, 5, 6)], "RMcast server restart with NAK test failed")
            self.assertTrue(self.__abort, "RMcast server restart with NAK test failed: didn't receive abort notification")
        finally:
            server.socket.close()
        
    def testLosslessRMcastServer ( self ):
        testData = "Echo!"
        port = 2009
        grp_addr = "224.0.0.1"
        msg = lambda s: bytes(testData + str(s), 'utf8')
        def testfunc ( s, b=0 ):
            for seq in range(1, 6): s.send(msg(b+seq))
            sleep(3)
            s.shutdown()
        def faketestfunc ( s, b ):
            for seq in range(1, 6): s.send(msg(b+seq))
        
        class TestRMcastServer ( RMcastServer ):
            def __init__ ( self, mcast_hostport, hostport, handler ):
                super(TestRMcastServer, self).__init__(mcast_hostport, hostport, handler, lossless=True)
                self.duprcvd = False
                
            def receive ( self, msg, from_address ):
                #print("TestRMcastServer: received message %s from %s" % (msg, from_address))
                try:
                    rcvq = self.rcvq(from_address)
                    if len(msg.body) > 0 and msg.seq < rcvq.ack:
                        self.duprcvd = True
                except KeyError:
                    pass

                super(TestRMcastServer, self).receive(msg, from_address)
                
        files = os.listdir()
        for file in filter(lambda s: re.match("\(.+\)@\(.+\)\.\w+", s), files):
            print("Removing file %s" % file)
            os.remove(file)
        
        self.__msgq.clear()
        host = socket.gethostbyname(self.__hostaddr)
        
        print("Creating lossless RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = TestRMcastServer((grp_addr, port), (host, port), self)
        try:
            Timer(3, testfunc, args=(server,)).start()
            server.serve_forever()
        finally:
            server.socket.close()
        self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1,6)], "Lossless RMcast server test failed")
            
        print("Waiting 5s for lossless RMcastServer to clean up")
        sleep(5)
        
        print("Restarting lossless RMcastServer on interface %s bound to %s" % (host, grp_addr))
        server = TestRMcastServer((grp_addr, port), (host, port), self)
        try:
            baddr = TestRMcastServer.ntoi(socket.inet_aton(server.server_address[0]))
            Timer(3, TestRMcastServer.sendnak, args=(server,baddr,1)).start()
            Timer(6, testfunc, args=(server,5)).start()
            server.serve_forever()
        finally:
            server.socket.close()
        self.assertTrue(server.duprcvd, "Lossless RMcast server test failed")
        self.assertCountEqual(self.__msgq, [msg(seq) for seq in range(1,11)], "Lossless RMcast server test failed")
        
    def testProtocolAgent ( self ):
        def testfunc ( s, testmsgs ):
            for msg in testmsgs: s.send(msg, s.address())
            sleep(3)
            s.shutdown()
        def faketestfunc ( s, testmsgs ):
            for msg in testmsgs: s.send(msg, s.address())
            s.peers[s.address()].close()
            sleep(3)
            s.shutdown()
            
        @ProtocolAgent.local
        class AgentTestLocal ( deque ):
            TestMsg = namedtuple('TestMsg', 'a,b')
            
            @ProtocolAgent.handles('TestMsg')
            def testHandler ( self, msg, src ):
                self.append(msg)

        testmsgs = [AgentTestLocal.TestMsg(a=1, b='Hi'), AgentTestLocal.TestMsg(a=2, b='there!')]
        agent = AgentTestLocal()
        for msg in testmsgs: agent.send(msg, agent.address())
        self.assertListEqual(testmsgs, list(agent), "Lists not equal")
        
        @ProtocolAgent.TCP
        class AgentTestRemote1 ( deque ):
            TestMsg = namedtuple('TestMsg', 'a,b')
            mutex = Lock()
            
            @ProtocolAgent.handles('TestMsg')
            def testHandler ( self, msg, src ):
                with self.mutex:
                    self.append(msg)
        
            def closed ( self, peer ):
                print("Peer %s closed connection" % str(peer))
                
        testmsgs = [AgentTestRemote1.TestMsg(a=1, b='Hi'), AgentTestRemote1.TestMsg(a=2, b='there!')]
        host = socket.gethostbyname(self.__hostaddr)
        port = 2010
        agent = AgentTestRemote1((host, port))
        try:
            Timer(3, testfunc, args=(agent,testmsgs)).start()
            agent.serve_forever()
            self.assertListEqual(testmsgs, list(agent), "Lists not equal")
        finally:
            agent.close()

        @ProtocolAgent.UDP
        class AgentTestRemote2 ( deque ):
            TestMsg = namedtuple('TestMsg', 'a,b')
            mutex = Lock()
                
            @ProtocolAgent.handles('TestMsg')
            def testHandler ( self, msg, src ):
                with self.mutex:
                    self.append(msg)
        
        testmsgs = [AgentTestRemote2.TestMsg(a=1, b='Hi'), AgentTestRemote2.TestMsg(a=2, b='there!')]
        host = socket.gethostbyname(self.__hostaddr)
        port = 2011
        agent = AgentTestRemote2((host, port))
        try:
            Timer(3, testfunc, args=(agent,testmsgs)).start()
            agent.serve_forever()
            self.assertListEqual(testmsgs, list(agent), "Lists not equal")
        finally:
            agent.socket.close()
        
        @ProtocolAgent.RMcast
        class AgentTestRemote3 ( deque ):
            TestMsg = namedtuple('TestMsg', 'a,b')
            mutex = Lock()
            
            @ProtocolAgent.handles('TestMsg')
            def testHandler ( self, msg, src ):
                with self.mutex:
                    self.append(msg)
        
        testmsgs = [AgentTestRemote3.TestMsg(a=1, b='Hi'), AgentTestRemote3.TestMsg(a=2, b='there!')]
        host = socket.gethostbyname(self.__hostaddr)
        mcasthost = "224.0.0.1" 
        port = 2012
        agent = AgentTestRemote3((mcasthost, port), (host, port))
        try:
            Timer(3, testfunc, args=(agent,testmsgs)).start()
            agent.serve_forever()       
            self.assertListEqual(testmsgs, list(agent), "Lists not equal")
        finally:
            agent.socket.close()
        
    def testRepeatableTimer ( self ):
        testData = 'Hi there!'
        
        timer = RepeatableTimer(1, ServerTest.handle, (self, testData, self), {}, 10)
        timer.start()
        sleep(12)
        self.assertListEqual(list(self.__msgq), [testData]*10, "Lists not equal")

        self.__msgq.clear()
        
        timer = RepeatableTimer(1, ServerTest.handle, (self, testData, self), {}, 10)
        timer.start()
        sleep(5)
        timer.cancel()
        sleep(5)
        self.assertListEqual(list(self.__msgq), [testData]*5, "Lists not equal")
        
        self.__msgq.clear()
    
    def testStateXferAgent ( self ):
        host = socket.gethostbyname(self.__hostaddr)
        port = 2012
        
        print("Creating state xfer agents on interface %s and ports %d and %d" % (host, port, port+1))
        stateA, stateB = dict(zip(range(5), range(4, -1, -1))), dict()
        agentA, agentB = StateXferAgent((host, port), stateA), StateXferAgent((host, port+1), stateB)
        try:
            Timer(3, lambda a,b: a.xferState(b.address()), args=(agentA, agentB)).start()
            agentB.serve_forever()       
            self.assertDictEqual(agentA.state, agentB.state, "States not equal")
        finally:
            agentA.socket.close()
            agentB.shutdown()
            agentB.socket.close()
        
    def testLogicalClockServer ( self ):
        testCommand = "echo"
        port = 2013
        grp_addr = "224.0.0.1"
        cmd = lambda s: testCommand + str(s)        
        def testfunc ( s ):
            for seq in range(1, 11): s.execute(cmd(seq))
            sleep(5)
            s.shutdown()
            
        host = socket.gethostbyname(self.__hostaddr)
        print("Creating LogicalClockServer on interface %s bound to %s" % (host, grp_addr))
        server = LogicalClockServer((grp_addr, port), (host, port), state_hostport=(host, 2021), death_time=2)
        try:
            Timer(5, testfunc, args=(server,)).start()
            server.serve_forever()
            self.assertListEqual([cmd(seq) for seq in range(1, 11)], [cmd for cmd in server], "Lists not equal")
        finally:
            server.socket.close()
        self.assertDictEqual(server.members, {}, "Bye msg not received or not handled properly")

    @unittest.skip("Not fully implemented yet")
    def testManyLogicalClockServers ( self ):
        if os.name != 'posix': return
        ADDRESS_BASE = '192.168.0.100'
        NUM_OF_SERVERS = 10
        PACKET_DELAY_MS = 10
        PACKET_LOSS_RATE = 10E-03
        
        # set-up ettercap to simulate the given delay and packet loss rate
        self.setup_ettercap(PACKET_DELAY_MS, PACKET_LOSS_RATE)
        
        # launch the test server processes getting their respective outputs as strings
        out = [\
            self.launch_testserver(socket.inet_ntoa(socket.inet_aton(ADDRESS_BASE) + server))
            for server in range(NUM_OF_SERVERS)
        ]
            
        # Wait 10s
        sleep(10)
        
        # compare the values
        reduce(lambda a,b: self.assertMultiLineEqual(a,b) or a, out)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testMcastServer ']
    unittest.main()
