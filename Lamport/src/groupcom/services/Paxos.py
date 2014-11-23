'''
Created on Aug 2, 2014

@author: ecejjar

An implementation of Lamport's Paxos protocol.
'''

from collections import namedtuple
from groupcom import server
from groupcom.services import LeaderElection
from math import ceil
from threading import Timer
from queue import Queue
import shelve
import logging

logger = logging.getLogger(__name__)

class Proposer(object):
    '''
    This class implements the Paxos' proposer role.
    During run-time, it handles an infinite number of Paxos instances. For those instances
    contended by other proposers it contains a proposal context, otherwise the instance
    requires no state in the proposer.
    The class is *not* thread-safe. There are race conditions lurking on internal variables.
    '''
    
    # Context of an outstanding proposal
    # n = proposal number
    # h = highest number within latest accepted proposals reported by acceptors
    # v = value chosen by acceptors for proposal h
    # q = list of acceptors (grows during prepare phase, decreases during accept phase)
    # t = timer for bounding the prepare and accept phases  
    ProposalCtx = namedtuple('ProposalCtx', 'n,h,v,q,t')
    
    def __init__ ( self, queue, timeout=2, n=0 ):
        '''
        Constructor
        @param queue: the queue this proposer gets its proposals from
        @param timeout: time-out limiting the prepare and accept phases of the protocol (default: 2s)
        @param n: value to which the proposal counter for every new proposal shall be initialized
        '''
        self.__queue = queue
        self.__proposed = None      # Proposals outstanding
        self.__contended = []       # List of ProposalCtx instances for contended proposals
        self.__timeout = timeout
        self.__n = n
    
    @property
    def n ( self ):
        '''The highest proposal issued by this proposer'''
        return self.__n

    @n.setter
    def set_n ( self, n ):
        #with self.lock_n:
        self.__n = n
        #self.__shelf[Proposer.PAXOS_PROPOSER_N] = n
        #self.__shelf.sync()
        
    def inc_n ( self ):
        #with self.lock_n:
        self.__n += 1
        #self.__shelf[Proposer.PAXOS_PROPOSER_N] = self.__n
        #self.__shelf.sync()
        return self.__n
        
    def propose ( self, i, value ):
        '''
        Send a new proposal to all known peers.
        Start a time-out just in case a quorum is never reached.
        @param i: Paxos instance(s) for which the proposal is being issued 
        @param value: the value proposed (None means no value is proposed)
        '''        
        self.__curctx = type(self).ProposalCtx(
            i,
            self.inc_n(),
            0,
            value,
            [],
            Timer(self.__timeout, type(self).abort, args=(self,)))
        msg = type(self).PrepareMsg(self.__curctx.n, self.__curctx.v)
        logger.debug(
            "%s: process %d sending proposal number %d to %d peers",
            type(self).__name__, self.__elector.p, msg.n, self.__elector.n )
        rcvrlist = map(lambda rcvr: self.send(msg, rcvr), self.__elector.peers)
        try:
            if any(rcvrlist):
                logger.debug(
                    "Proposer %d failed sending proposal to one or more acceptors",
                    self.__elector.p)
        except Exception as e:
            logger.debug(
                "Proposer %d failed sending proposal to one or more peers, error: ",
                self.__elector.p, e)
        finally:
            self.__curctx.t.start()
            return self.__curctx

    def abort ( self ):
        logger.debug("Proposer %d aborting proposal %d" % (self.__elector.p, self.__n))
        self.__curctx.t.cancel()
        self.__curctx = None
        
    @server.ProtocolAgent.handles('PrepareRspMsg')
    def handlePrepareRsp (self, msg, src):
        if self.__curctx and msg.n < self.__curctx.n:
            logger.debug(
                "Proposer %d received response to expired proposal %d from %s",
                self.__elector.p, msg.n, src)
            return
        
        if not msg.ok:
            logger.debug(
                "Acceptor %s at proposal %d denied proposal %d from proposer %d",
                src, msg.l, self.__curctx.n, self.__elector.p)
            
            # Update proposal status if necessary
            if msg.l > self.__curctx.h:
                # Some other proposer has issued a proposal higher than ours. Give up and
                # update our status so our chances of succeeding next time are better.
                self.n = msg.l + 1
                self.__curctx = None
                raise Exception("Proposal aborted, another proposer got ahead of us")            
        
        self.__curctx.q.append(src)
        if len(self.__curctx.q) >= ceil(self.__elector.p/2+1):
            # We got a quorum, restart timer and send accept request to all acceptors
            self.__curctx.t.cancel()
            msg = type(self).AcceptMsg(self.__curctx.n, self.__curctx.v)
            rcvrlist = map(lambda rcvr: self.send(msg, rcvr), self.__elector.peers)
            try:
                if any(rcvrlist):
                    logger.debug(
                        "Proposer %d failed sending accept command to one or more acceptors",
                        self.__elector.p)
            except Exception as e:
                logger.error(
                    "Proposer %d failed sending accept command to one or more peers, error: ",
                    self.__elector.p, e)
            finally:
                self.__curctx.t.finished.clear()
                self.__curctx.t.run()

    @server.ProtocolAgent.handles('PrepareRspMsg')
    def handleAcceptRsp (self, msg, src):
        if self.__curctx is None:
            if self.__elector.isLeader:
                logger.debug(
                    "Proposer %d received unexpected accept response %s from %s",
                    self.__elector.p, msg, src)
            else:
                logger.debug(
                    "Proposer %d received accept response %s from %s but it's not leader anymore",
                    self.__elector.p, msg, src)
            return
            
        if msg.n != self.__curctx.n:
            logger.debug(
                "Proposer %d received response to expired accept command %d from %s",
                self.__elector.p, msg.n, src)
            return
        
        if not msg.ok:
            logger.debug(
                "Acceptor %s refused accepting proposal %d from proposer %d",
                src, self.__curctx.n, self.__elector.p)
            return
        
        try:
            self.__curctx.q.remove(src)
        except ValueError:
            logger.debug(
                "Proposer %d received accept response %d from %s but prepare response unseen",
                self.__elector.p, msg.n, src)
        if len(self.__curctx.q) == 0:
            self.__curctx.t.cancel()
            self.__curctx = None
            logger.info(
                "Proposer %d got proposal %n accepted by a majority of acceptors",
                self.__elector.p, msg.n)
        

@server.ProtocolAgent.UDP
class Acceptor ( object ):
    '''
    This class implements the Paxos' acceptor role
    '''
    PAXOS_ACCEPTOR_N = 'paxos_acceptor_n'
    PAXOS_ACCEPTOR_H = 'paxos_acceptor_h'
    PrepareMsg = namedtuple('PrepareMsg', 'n')
    PrepareRspMsg = namedtuple('PrepareRspMsg', 'n,ok,l,v')
    AcceptMsg = namedtuple('AcceptMsg', 'n,v')
    AcceptRspMsg = namedtuple('AcceptRspMsg', 'ok,n')
    
    def __init__ ( self, n, h, proposal_checker=None ):
        '''
        Constructor
        '''
        self.__n = n
        self.__h = h
        self.__proposal_checker = proposal_checker
        self.__v = None
                
    @property
    def n ( self ):
        '''The highest proposal accepted by this acceptor'''
        return self.__n

    @property
    def h ( self ):
        '''The highest prepare request answered by this acceptor (invariant h >= n must hold)'''
        return self.__h

    @n.setter
    def set_n ( self, n ):
        '''
        Increases n atomically
        '''
        #with lock_n:
        self.__n = n
        #self.__shelf[Acceptor.PAXOS_ACCEPTOR_N] = self.__n
        #self.__shelf.sync()
        
    @h.setter
    def set_h ( self, h ):
        '''
        Increases h atomically
        '''
        #with lock_n:
        self.__h = h
        #self.__shelf[Acceptor.PAXOS_ACCEPTOR_H] = self.__h
        #self.__shelf.sync()
        
    @property
    def v ( self ):
        '''The value of the highest proposal accepted by this acceptor'''
        return self.__v

    @server.ProtocolAgent.handles('PrepareMsg')
    def handlePrepare (self, msg, src):
        # If we've already responded to a prepare request with a highest number
        # this request shall never end up in a successful accept
        if msg.n <= self.__h:
            logger.debug(
                "Acceptor at proposal %d ignoring proposal with number %d from proposer %s",
                self.__h, msg.n, src)
            # Should we tell the proposer if msg.n < self.__h so he can abandon the proposal?
            # Notice if that's the case for a number of acceptors, each acceptor would
            # generate one message to the proposer. Might limit scalability.
            return
        
        if msg.n <= self.__n or 0:
            logger.debug(
                "Acceptor having accepted %d ignoring proposal with number %d from proposer %s",
                self.__n, msg.n, src)
            return
        
        if self.__proposal_checker and not self.__proposal_checker.check(msg.v):
            return type(self).PrepareRspMsg(msg.n, False, self.__n, self.__v)
        
        self.h = msg.n
        return type(self).PrepareRspMsg(msg.n, True, self.__n, self.__v)
            
    @server.ProtocolAgent.handles('AcceptMsg')
    def handleAccept (self, msg, src):
        # We might have received a higher-numbered proposal since we prepared for this one
        if msg.n < self.__h:
            logger.debug(
                "Acceptor at proposal %d refusing accept %d from proposer %s",
                self.__h, msg.n, src)
            return type(self).AcceptRspMsg(msg.n, False)
        
        self.n = msg.n
        self.__v = msg.v
        logger.debug(
            "Acceptor at proposal %d accepted %d from proposer %s",
            self.__h, msg.n, src)
        return type(self).AcceptRspMsg(msg.n, True)


@server.ProtocolAgent.UDP
class Learner(object):
    '''
    This class implements the Paxos' learner role
    '''
    AgreedMsg = namedtuple('AgreedMsg', 'v')
    
    def __init__ ( self ):
        '''
        Constructor
        '''
        self.__v = None

    @property
    def v ( self ):
        '''The value of the proposal reported as accepted by the proposer'''
        return self.__v

    @server.ProtocolAgent.handles('AgreedMsg')
    def handleAgreed ( self, msg ):
        self.__v = msg.v

@server.ProtocolAgent.UDP
class PaxosAgent ( object ):
    N = 0                                                   # Holds the internal class-wide proposal counter
    PAXOS_PROPOSER_N = 'paxos_proposer_n'                   # Key to access proposer persistent data in shelf
    PAXOS_ACCEPTOR_N = 'paxos_acceptor_n'                   # Key to access acceptor persistent data in shelf
    PAXOS_ACCEPTOR_H = 'paxos_acceptor_h'                   # Key to access acceptor persistent data in shelf
    PrepareMsg = namedtuple('PrepareMsg', 'n,v')            # n = proposal number, v = proposed value
    PrepareRspMsg = namedtuple('PrepareRspMsg', 'n,ok,l,v') # n = proposal number, ok = outcome (T/F), l = latest proposal accepted, v = latest value chosen
    AcceptMsg = namedtuple('AcceptMsg', 'n,v')              # n = proposal number, v = value to choose
    AcceptRspMsg = namedtuple('AcceptRspMsg', 'n,ok')       # n = proposal number, ok = outcome (T/F)
    
    @classmethod
    def pickN ( cls ):
        # TODO: make atomic; TIP use a lock-less queue to store pre-seeded n's
        cls.N += 1
        return cls.N
        
    def __init__ ( self, a, timeout=2, le_port=2468, le_timeout=0.2, le=None ):
        '''
        Constructor
        @param timeout: time-out limiting the prepare and accept phases of the protocol (default: 2s)
        @param le_port: local port used for the Leader Election algorithm (default: 2468)
        @param le_timeout: time-out interval for the Leader Election algorithm (2*d in case of Stable Leader Election)
        @param le: alternate Leader Election implementation (default: None uses O(1) Stable Leader Election) 
        '''
        self.__elector = le or LeaderElection.O1StableLeaderElector(
            (self.address[0], le_port), timeout=le_timeout, observer=self)
        
        # Recover the highest n used from stable storage
        try:
            filename = self.grpaddr + '@' + self.address + '.dat'
        except AttributeError:
            filename = self.address + '.dat'

        try:
            self.__shelf = shelve.open(filename)
            prp_n = self.__shelf[Proposer.PAXOS_PROPOSER_N]
            acc_n = self.__shelf[Acceptor.PAXOS_ACCEPTOR_N]
            acc_h = self.__shelf[Acceptor.PAXOS_ACCEPTOR_H]
        except KeyError:
            prp_n = type(self).pickN()
            acc_n = None
            acc_h = None
        except Exception as e:
            raise e    # TODO: create an exception for this problem and raise it here

        self.__proposer = Proposer(timeout, prp_n)
        self.__acceptor = Acceptor(acc_n, acc_h)
        self.__learner  = Learner()
        self.__queue = Queue(a)
                
    @property
    def elector ( self ):
        return self.__elector
    
    @property
    def proposer ( self ):
        return self.__proposer
    
    @property
    def acceptor ( self ):
        return self.__acceptor
    
    def executeAsync ( self, command, observer ):
        '''
        Orders reliable execution of a command in the process ensemble.
        Outcome shall be notified to observer.
        '''
        if not self.__elector.isLeader:
            # TODO: forward command to leader process
            raise Exception(
                "Cannot execute command, I'm process %d and current leader is %d" %
                (self.__elector.p, self.__elector.leader)) 
        else:
            self.__queue.put((command, observer))
    
    def execute ( self, command, timeout=None ):
        '''
        Execute a command synchronously and reliably in the process ensemble.
        '''
        if not self.__elector.isLeader:
            # TODO: forward command to leader process
            raise Exception(
                "Cannot execute command, I'm process %d and current leader is %d" %
                (self.__elector.p, self.__elector.leader)) 
        else:
            self.__queue.put(command, timeout=timeout)
            # Wait on learner
            
    @server.ProtocolAgent.handles('PrepareRspMsg')
    def handlePrepareRsp (self, msg, src):
        if not self.__proposer.active:
            if self.__elector.isLeader:
                logger.debug(
                    "Proposer %d received unexpected proposal response %s from %s",
                    self.__elector.p, msg, src)
            else:
                logger.debug(
                    "Proposer %d received proposal response %s from %s but it's not leader anymore",
                    self.__elector.p, msg, src)
            return
            
'''
Specialized shutdown method
'''
def _shutdown ( self ):
    self.__shelf.close()
    super(type(self), self).shutdown()
PaxosAgent.shutdown = _shutdown    
