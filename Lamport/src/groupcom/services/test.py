'''
Created on 08/03/2013

@author: ecejjar
'''

import FileCaster, LeaderElection, Paxos
import unittest
import socket
import logging
import time
import random
from time import sleep
from threading import Thread, Timer
from functools import wraps
from itertools import count
from groupcom.server import RepeatableTimer

if not hasattr(unittest, 'skip'):
    unittest.skip = lambda func: func   # Python 3.0 and lower
    
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)


class FileCasterTestReceiver:
    '''
    To be run in remote computer before running the unit tests below
    '''
    pass


class FileCasterTest(unittest.TestCase):

    @unittest.skip("Unfinished")
    def testCastFile(self):
        testFile = "test.py"
        port = 2000
        grp_addr = "224.0.0.1"
        host = socket.gethostbyname(socket.gethostname())
        print("Creating FileCaster on interface %s bound to %s" % (host, grp_addr))
        caster = FileCaster((host, port), (grp_addr, port), 32)
        caster.cast(testFile)
        caster.serve_forever()

        
class LeaderElecionTest(unittest.TestCase):

    def setUp ( self ):
        self.__leader = {}
        
        # NOTE: in Linux when connected over WLAN,
        # if I let the kernel pick the IP address to use no multicast message is received
        self.__hostaddr = socket.gethostname()
        
    def notify ( self, elector ):
        print("Elector at %s notifies its current leader is %s" % (elector.server_address, elector.leader))
        self.__leader[elector.server_address] = elector.leader
                        
    #@unittest.skip("Long test")
    def testLeaderElection ( self ):
        def testfunc ( s ):
            s.serve_forever()

        NUM_OF_PEERS = 5 #10
        BASE_PORT = 2020
        host = socket.gethostbyname(self.__hostaddr)
        d = 0.2

        print(
            "=======================================================================\n" +
            " Starting testLeaderElection\n" +
            "-----------------------------------------------------------------------\n")
        
        addresses = [(host, port) for port in range(BASE_PORT, BASE_PORT+NUM_OF_PEERS)]
        peers = [LeaderElection.LeaderElector(addr, peers=addresses, timeout=d, observer=self)
                 for addr in addresses]
        threads = [Thread(target=testfunc, args=(peer,), name="LeaderElector@%s:%d" % peer.server_address)
                   for peer in peers]
        #addresses = map(lambda port: (host, port), range(BASE_PORT, BASE_PORT+NUM_OF_PEERS))
        #peers = map(lambda addr: LeaderElection.StableLeaderElector(addr, peers=addresses, timeout=0.1, observer=self), addresses)
        #threads = map(lambda peer: Thread(target=testfunc, args=(peer,), name="%s:%d" % peer.server_address), peers)
        try:
            print("Starting %d basic leader electors on ports %d to %d" % (NUM_OF_PEERS, BASE_PORT, BASE_PORT+NUM_OF_PEERS-1))
            for thread in threads: thread.start()
            print("Waiting for electors to agree on a leader, you should see some console messages...")
            sleep(3)
            l = next(iter(self.__leader.values()))
            self.assertSameElements(
                addresses, self.__leader.keys(),
                "Something went wrong, some elector didn't notify its observer")
            self.assertNotIn(
                None, self.__leader.values(),
                "Something went wrong, some elector has None as leader")
            self.assertListEqual(
                [l]*NUM_OF_PEERS, list(self.__leader.values()),
                "Something went wrong, some elector disagreed in who's leader")
            print("Electors agreed on peer %d, halting its thread" % l)
            peers[l].shutdown()
            peers[l].socket.close()
            sleep(3)
            self.assertNotIn(
                l, filter(lambda p: p != l, self.__leader.values()),
                "Some elector stuck on leader %d" % l)
            print("Restarting old leader %d" % l)
            # Do not reuse peers[l], its round and current leader are set to those when it was shutdown
            peers[l] = LeaderElection.LeaderElector(addresses[l], peers=addresses, timeout=0.1, observer=self)
            threads[l] = Thread(target=testfunc, args=(peers[l],), name="LeaderElector@%s:%d" % peers[l].server_address)
            threads[l].start()
            sleep(3)
            l = next(iter(self.__leader.values()))
            self.assertListEqual(
                [l]*NUM_OF_PEERS, list(self.__leader.values()),
                "Something went wrong, some elector disagreed in who's leader")
        finally:
            print("Shutting down all electors")
            for peer in peers:
                peer.shutdown()
                #peer.socket.close()
            print(
                "-----------------------------------------------------------------------\n" +
                " Ending testLeaderElection\n" +
                "=======================================================================\n")

    #@unittest.skip("Unfinished")
    def testDynamicMembership ( self ):
        def testfunc ( s ):
            s.serve_forever()
            
        print(
            "=======================================================================\n" +
            " Starting testDynamicMembership\n" +
            "-----------------------------------------------------------------------\n")
        
        NUM_OF_PEERS = 5 #10
        BASE_PORT = 2020
        host = socket.gethostbyname(self.__hostaddr)
        d = 0.2
        
        addresses = [(host, port) for port in range(BASE_PORT, BASE_PORT+NUM_OF_PEERS)]
        peers = [LeaderElection.LeaderElector(addr, peers=addresses[:2], timeout=d, observer=self)
                 for addr in addresses]
        threads = [Thread(target=testfunc, args=(peer,), name="LeaderElector@%s:%d" % peer.server_address)
                   for peer in peers]

        try:
            print("Starting first leader elector on port %d" % BASE_PORT)
            threads[0].start()
            sleep(3)
            print("Starting second leader elector on port %d" % (BASE_PORT+1))
            threads[1].start()
            print("Waiting for electors to agree on a leader, you should see some console messages...")
            sleep(3)
            self.assertNotIn(
                None, self.__leader.values(),
                "Something went wrong, some elector has None as leader")
            self.assertSameElements(
                addresses[:2], self.__leader.keys(),
                "Something went wrong, some elector didn't notify its observer")
            print("Starting third to fifth leader electors on ports %d-%d" % (BASE_PORT+2, BASE_PORT+NUM_OF_PEERS))
            for thread in threads[2:]: thread.start()
            print("Waiting for electors to agree on a leader, you should see some console messages...")
            sleep(3)
            l = next(iter(self.__leader.values()))
            self.assertNotIn(
                None, self.__leader.values(),
                "Something went wrong, some elector has None as leader")
            self.assertSameElements(
                addresses, self.__leader.keys(),
                "Something went wrong, some elector didn't notify its observer")
            self.assertListEqual(
                [l]*NUM_OF_PEERS, list(self.__leader.values()),
                "Something went wrong, some elector disagreed in who's leader")
            print("Electors agreed on peer %d" % l)
        finally:
            print("Shutting down all electors")
            for peer in peers:
                peer.shutdown()
                #peer.socket.close()
        
            print(
                "-----------------------------------------------------------------------\n" +
                " Ending testLeaderElection\n" +
                "=======================================================================\n")
            
    #@unittest.skip("Long test")
    def testStableLeaderElection ( self ):
        def testfunc ( s ):
            s.serve_forever()
            
        print(
            "=======================================================================\n" +
            " Starting testStableLeaderElection\n" +
            "-----------------------------------------------------------------------\n")
        
        NUM_OF_PEERS = 5
        BASE_PORT = 2030
        host = socket.gethostbyname(self.__hostaddr)
        d = 0.2
        M = 2   # The arbitrarily long time that link to&from peer 2 delays messages
        
        addresses = [(host, port) for port in range(BASE_PORT, BASE_PORT+NUM_OF_PEERS)]
        peers = [LeaderElection.StableLeaderElector(addr, peers=addresses, timeout=d, observer=self)
                 for addr in addresses]
        threads = [Thread(target=testfunc, args=(peer,), name="StableLeaderElector@%s:%d" % peer.server_address)
                   for peer in peers]
        #addresses = map(lambda port: (host, port), range(BASE_PORT, BASE_PORT+NUM_OF_PEERS))
        #peers = map(lambda addr: LeaderElection.StableLeaderElector(addr, peers=addresses, timeout=0.1, observer=self), addresses)
        #threads = map(lambda peer: Thread(target=testfunc, args=(peer,), name="%s:%d" % peer.server_address), peers)
        
        # For this test we need a special class of agent, one that
        # simulates a very slow link (link delay >> d).
        # Define a decorator that delays a function call.
        def delayed ( func ):
            @wraps(func)
            def wrapper ( *args, **kwargs ):
                Timer(M, func, args, kwargs).start()
            return wrapper

        # Right after starting round 1 (time 2d+2), process 2 crashes.
        # Define a decorator for startRound() that shuts down the peer if the round is 1 
        def crashing ( func ):
            @wraps(func)
            def wrapper ( r ):
                result = func(r)
                if r == 1:
                    func.__self__.shutdown()
                    func.__self__.socket.close()
                return result
            return wrapper
        
        # Now replace the second peer's send(), handle() and startRound() by the modified ones.
        # This has two consequences:
        # 1. the second peer does not agree on 0 as leader until after a long time (>> d)
        # 2. the second peer times out on 0 and starts round 1, but it takes a long time
        #    (>> d) for the other peers to realize
        # 3. the second peer does not react to any other message after starting round 1
        # The outcome is that 0 is demoted and 1 promoted without apparent reason!!
        peers[2].handle = delayed(peers[2].handle)
        peers[2].send = delayed(peers[2].send)
        peers[2].startRound = crashing(peers[2].startRound)

        try:
            print("Starting %d stable leader electors on ports %d to %d" % (NUM_OF_PEERS, BASE_PORT, BASE_PORT+NUM_OF_PEERS-1))
            for thread in threads: thread.start()
            print("Waiting for electors to agree on a leader, you should see some console messages...")
            sleep(M/2)
            l = self.__leader[addresses[0]]
            print("Electors agreed on peer %s" % l)
            self.assertSameElements(
                addresses, self.__leader.keys(),
                "Something went wrong, some elector didn't notify its observer")
            self.assertListEqual(
                [l]*2 + [None] + [l]*(NUM_OF_PEERS-3),
                [self.__leader[address] for address in addresses],
                "Something went wrong, leader has been demoted")
            sleep(M/2+1)
            # We cannot make any guarantee here. Some peer might be in the process of electing its leader
            # hence it shall have reported None as current leader. 
            #l = next(iter(self.__leader.values()))
            #self.assertListEqual(
            #    [l]*(NUM_OF_PEERS-1),
            #    list(map(lambda p: p[1], filter(lambda p: p[0] != addresses[2], self.__leader.items()))),
            #    "Something went wrong after M seconds, working peers didn't agree on the same leader")
            del(peers[2].handle)
            del(peers[2].send)
            del(peers[2].startRound)
            sleep(M+2)  # need to wait at least M/2+M/2+1 for all pending messages from peer 2 to arrive
            l = self.__leader[addresses[0]]
            self.assertListEqual(
                [l]*2 + [None] + [l]*(NUM_OF_PEERS-3),
                [self.__leader[address] for address in addresses],
                "Something went wrong, some elector disagreed in who's leader")
        finally:
            print("Shutting down all electors")
            for peer in peers:
                peer.shutdown()
                #peer.socket.close()
                
            print(
                "-----------------------------------------------------------------------\n" +
                " Ending testStableLeaderElection\n" +
                "=======================================================================\n")

    @unittest.skip("Long test")
    def testExpiringLinks ( self ):
        print(
            "=======================================================================\n" +
            " Starting testExpiringLinks\n" +
            "-----------------------------------------------------------------------\n")
        
        BASE_PORT = 2040
        host = socket.gethostbyname(self.__hostaddr)
        peer = (host, BASE_PORT)
        explinks = LeaderElection.ExpiringLinksImpl()
        d = 0.2             # delay mu
        j = 0.01            # delay sigma
        t = time.time()+1   # offset mu (the +1 is necessary b/c RepeatableTimer won't trigger until the 1st time-out)
        e = 0.01            # offset sigma (drift)
        n = 10              # number of samples
        
        self.assertEqual(
            explinks.O(peer), LeaderElection.ExpiringLinksImpl.NO_INFO.offset,
            "ExpiringLinksImpl clock offset estimation for unknown peer not equal to NO_INFO.offset")
        self.assertEqual(
            explinks.D(peer), LeaderElection.ExpiringLinksImpl.NO_INFO.delay,
            "ExpiringLinksImpl network delay estimation for unknown peer not equal to NO_INFO.delay")
        
        # Prefab random network delays following normal distribution
        delays = [max(0.01, random.gauss(d,j)) for k in range(2*n)]

        print("Sending 1 ack message per second for 10s, please wait...")
        
        it = count()
        def testfunc ( e ):
            '''
            OK, here's the deal: the ExpiringLinksImpl class uses current time to measure O and D;
            therefore we need to adjust all the times so the simulation matches what we want to simulate:
            * sender issues one Start message every second (actual time includes local clock drift)
            * sender's clock is the local clock
            * receiver's clock starts at t (i.e. it is 0 at instant t, t is set on method entrance)
            * receiver always takes the same time to send the Ack: 0.01s (plus some local drift)
            Delays for every message/ack exchange are taken from a prefab normally-distributed sample.
            '''
            k = next(it)
            msg = LeaderElection.OnStableLeaderElector.StartMsg(time.time(), 0)
            sleep(delays[k])    # simulate network delay sender->receiver
            o = time.time()-t   # here we received the message
            sleep(0.01)         # simulate local processing at receiver
            l = time.time()-t   # here we send the ack
            ack = LeaderElection.OnStableLeaderElector.AckMsg(l, msg.timestamp, o, 0)
            sleep(delays[-k])   # simulate network delay receiver->sender
            print("Received Ack(ts=%f,msg_ts=%f,msg_rcv_ts=%f,round=%d) at %f" % (ack + (time.time(),)))
            return LeaderElection.ExpiringLinksImpl.processAckTimestamp(e, ack, peer)
        
        RepeatableTimer(1, testfunc, (explinks,), {}, n).start()
        sleep(n+2)
        self.assertTrue(
            d-3*j <= explinks.D(peer).avg and explinks.D(peer).avg <= d+3*j,
            "Estimated network delay not within three stddev from average %f: %f" % (d, explinks.D(peer).avg))
        self.assertTrue(
            t-3*e <= abs(explinks.O(peer).avg) and abs(explinks.O(peer).avg) <= t+3*e,
            "Estimated clock offset not within three stddev from average %f: %f" % (t, explinks.O(peer).avg))
                
        print(
            "-----------------------------------------------------------------------\n" +
            " Ending testExpiringLinks\n" +
            "=======================================================================\n")

    #@unittest.skip("Long test")
    def testOnStableLeaderElection ( self ):
        def testfunc ( s ):
            s.serve_forever()
            
        print(
            "=======================================================================\n" +
            " Starting testOnStableLeaderElection\n" +
            "-----------------------------------------------------------------------\n")

        NUM_OF_PEERS = 5
        BASE_PORT = 2050
        host = socket.gethostbyname(self.__hostaddr)
        d = 0.2
        
        addresses = [(host, port) for port in range(BASE_PORT, BASE_PORT+NUM_OF_PEERS)]
        peers = [LeaderElection.OnStableLeaderElector(addr, peers=addresses, timeout=d, observer=self)
                 for addr in addresses]
        threads = [Thread(target=testfunc, args=(peer,), name="OnStableLeaderElector@%s:%d" % peer.server_address)
                   for peer in peers]

        try:
            print("Starting %d O(n) stable leader electors on ports %d to %d" % (NUM_OF_PEERS, BASE_PORT, BASE_PORT+NUM_OF_PEERS-1))
            for thread in threads: thread.start()
            print("Waiting for electors to agree on a leader (%d+4 times %f), you should see some console messages..." % (NUM_OF_PEERS, d))
            sleep((NUM_OF_PEERS+4)*d)
            l = next(iter(self.__leader.values()))
            self.assertNotIn(
                None, self.__leader.values(),
                "Something went wrong, some elector has None as leader")
            self.assertSameElements(
                addresses, self.__leader.keys(),
                "Something went wrong, some elector didn't notify its observer")
            self.assertListEqual(
                [l]*NUM_OF_PEERS, list(self.__leader.values()),
                "Something went wrong, some elector disagreed in who's leader")
            print("Electors agreed on peer %d" % l)
        finally:
            print("Shutting down all electors")
            for peer in peers:
                peer.shutdown()
                #peer.socket.close()

            print(
                "-----------------------------------------------------------------------\n" +
                " Ending testOnStableLeaderElection\n" +
                "=======================================================================\n")
        
    #@unittest.skip("Long test")
    def testO1StableLeaderElection ( self ):
        def testfunc ( s ):
            s.serve_forever()
            
        print(
            "=======================================================================\n" +
            " Starting testO1StableLeaderElection\n" +
            "-----------------------------------------------------------------------\n")

        NUM_OF_PEERS = 5
        BASE_PORT = 2060
        host = socket.gethostbyname(self.__hostaddr)
        d = 0.2
        
        addresses = [(host, port) for port in range(BASE_PORT, BASE_PORT+NUM_OF_PEERS)]
        peers = [LeaderElection.O1StableLeaderElector(addr, peers=addresses, timeout=d, observer=self)
                 for addr in addresses]
        threads = [Thread(target=testfunc, args=(peer,), name="O1StableLeaderElector@%s:%d" % peer.server_address)
                   for peer in peers]

        try:
            print("Starting %d O(1) stable leader electors on ports %d to %d" % (NUM_OF_PEERS, BASE_PORT, BASE_PORT+NUM_OF_PEERS-1))
            for thread in threads: thread.start()
            print("Waiting for electors to agree on a leader (6 times %f), you should see some console messages..." % d)
            sleep(6*d)
            l = next(iter(self.__leader.values()))
            self.assertNotIn(
                None, self.__leader.values(),
                "Something went wrong, some elector has None as leader")
            self.assertSameElements(
                addresses, self.__leader.keys(),
                "Something went wrong, some elector didn't notify its observer")
            self.assertListEqual(
                [l]*NUM_OF_PEERS, list(self.__leader.values()),
                "Something went wrong, some elector disagreed in who's leader")
            print("Electors agreed on peer %d" % l)
        finally:
            print("Shutting down all electors")
            for peer in peers:
                peer.shutdown()
                peer.socket.close()
                
            print(
                "-----------------------------------------------------------------------\n" +
                " Ending testO1StableLeaderElection\n" +
                "=======================================================================\n")
    
    #@unittest.skip("Unfinished")
    def testPaxos ( self ):
        def testfunc ( s ):
            s.serve_forever()
            
        print(
            "=======================================================================\n" +
            " Starting testPaxos\n" +
            "-----------------------------------------------------------------------\n")

        BASE_PORT = 2070
        host = socket.gethostbyname(self.__hostaddr)
        value = "Hi there!"
        
        proposer = Paxos.Proposer((host, BASE_PORT))
        acceptor = Paxos.Acceptor((host, BASE_PORT+1))
        listener = Paxos.Learner()
        
        peers = [proposer, acceptor, listener]
        threads = [Thread(
                        target=testfunc,
                        args=(peer,),
                        name="PaxosAgent_%s:%d" % (type(peer).__name__, peer.server_address))
                   for peer in peers]
        
        try:
            for thread in threads: thread.start()
            print("Waiting for agents to agree on a leader, you should see some console messages")
            sleep(3)
            self.assertTrue(proposer.elector.isLeader, "Proposer was not elected leader")
            print("Proposer proposing value %s to Acceptor" % value)
            proposer.propose(value)
            print("Waiting for acceptor to accept the proposal")
            sleep(3)
            self.assertEqual(value, acceptor.v, "Acceptor did not accept the proposal of value %s" % value)
        finally:
            print("Shutting down all electors")
            for peer in peers:
                peer.shutdown()
                peer.socket.close()
                
        print(
            "-----------------------------------------------------------------------\n" +
            " Ending testPaxos\n" +
            "=======================================================================\n")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()