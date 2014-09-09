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
import shelve
import logging

logger = logging.getLogger(__name__)

'''
Common shutdown method for Acceptors and Proposers
'''
def _shutdown ( self ):
    self.__shelf.close()
    super(type(self), self).shutdown()
    
@server.ProtocolAgent.UDP
class Proposer(object):
    '''
    This class implements the Paxos' proposer role.
    The class is *not* thread-safe. There are race conditions lurking on internal variables.
    '''
    N = 0                                                   # Holds the internal class-wide proposal counter
    PAXOS_PROPOSER_N = 'paxos_proposer_n'                   # Key to access proposer persistent data in shelf
    PrepareMsg = namedtuple('PrepareMsg', 'n,v')            # n = proposal number, v = proposed value
    PrepareRspMsg = namedtuple('PrepareRspMsg', 'n,ok,l,v') # n = proposal number, ok = outcome (T/F), l = latest proposal accepted, v = latest value chosen
    AcceptMsg = namedtuple('AcceptMsg', 'n,v')              # n = proposal number, v = value to choose
    AcceptRspMsg = namedtuple('AcceptRspMsg', 'n,ok')       # n = proposal number, ok = outcome (T/F)
    
    # Context of an outstanding proposal
    # n = proposal number
    # h = highest number within latest accepted proposals reported by acceptors
    # v = value chosen by acceptors for proposal h
    # q = list of acceptors (grows during prepare phase, decreases during accept phase)
    # t = timer for bounding the prepare and accept phases  
    ProposalCtx = namedtuple('ProposalCtx', 'n,h,v,q,t')
    
    @classmethod
    def pickN ( cls ):
        # TODO: make atomic; TIP use a lock-less queue to store pre-seeded n's
        cls.N += 1
        return cls.N
        
    def __init__ ( self, timeout=2, le_port=2468, le_timeout=0.2, le=None ):
        '''
        Constructor
        @param timeout: time-out limiting the prepare and accept phases of the protocol (default: 2s)
        @param le_port: local port used for the Leader Election algorithm (default: 2468)
        @param le_timeout: time-out interval for the Leader Election algorithm (2*d in case of Stable Leader Election)
        @param le: alternate Leader Election implementation (default: None uses O(1) Stable Leader Election) 
        '''
        self.__timeout = timeout
        self.__elector = le or \
            LeaderElection.O1StableLeaderElector(
                (self.address[0], le_port), timeout=le_timeout, observer=self)
        self.__curctx = None
        
        # Recover the highest n used from stable storage
        try:
            filename = self.grpaddr + '@' + self.address + '.dat'
        except AttributeError:
            filename = self.address + '.dat'
        
        try:
            self.__shelf = shelve.open(filename)
            self.__n = self.__shelf[Proposer.PAXOS_PROPOSER_N]
            logger.debug("Proposer %s initialized with proposal number %d", self.address, self.__n)
        except KeyError:
            self.__n = type(self).pickN()
        except Exception as e:
            raise e    # TODO: create an exception for this problem and raise it here

    @property
    def n ( self ):
        '''The highest proposal issued by this proposer'''
        return self.__n

    @n.setter
    def set_n ( self, n ):
        #with self.lock_n:
        self.__n = n
        self.__shelf[Proposer.PAXOS_PROPOSER_N] = n
        self.__shelf.sync()
        
    def inc_n ( self ):
        #with self.lock_n:
        self.__n += 1
        self.__shelf[Proposer.PAXOS_PROPOSER_N] = self.__n
        self.__shelf.sync()
        return self.__n
        
    @property
    def elector ( self ):
        return self.__elector
    
    def propose ( self, value ):
        '''
        Send a new proposal to all known peers if I'm the current leader.
        Start a time-out just in case a quorum is never reached.
        '''
        if not self.__elector.isLeader:
            raise Exception(
                "Cannot send proposal, I'm process %d and current leader is %d" %
                (self.__elector.p, self.__elector.leader)) 

        # This implementation can have just one proposal outstanding.
        # TODO: allow multiple proposals to run concurrently        
        if self.__curctx:
            raise Exception(
                "Cannot send proposal, proposal %d is still outstanding" %
                (self.__curctx.n,))
        
        self.__curctx = \
            type(self).ProposalCtx(
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

    def abort ( self ):
        logger.debug("Proposer %d aborting proposal %d" % (self.__elector.p, self.__curctx.n))
        self.__curctx.t.cancel()
        self.__curctx = None
        
    @server.ProtocolAgent.handles('PrepareRspMsg')
    def handlePrepareRsp (self, msg, src):
        if self.__curctx is None:
            if self.__elector.isLeader:
                logger.debug(
                    "Proposer %d received unexpected proposal response %s from %s",
                    self.__elector.p, msg, src)
            else:
                logger.debug(
                    "Proposer %d received proposal response %s from %s but it's not leader anymore",
                    self.__elector.p, msg, src)
            return
            
        if msg.n != self.__curctx.n:
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
            logger.debug(
                "Proposer %d received unexpected accept response %s from %s",
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
        
Proposer.shutdown = _shutdown


@server.ProtocolAgent.UDP
class Acceptor(object):
    '''
    This class implements the Paxos' acceptor role
    '''
    PAXOS_ACCEPTOR_N = 'paxos_acceptor_n'
    PAXOS_ACCEPTOR_H = 'paxos_acceptor_h'
    PrepareMsg = namedtuple('PrepareMsg', 'n')
    PrepareRspMsg = namedtuple('PrepareRspMsg', 'n,ok,l,v')
    AcceptMsg = namedtuple('AcceptMsg', 'n,v')
    AcceptRspMsg = namedtuple('AcceptRspMsg', 'ok,n')
    
    def __init__ ( self, proposal_checker=None ):
        '''
        Constructor
        '''
        # Recover the highest n and h seen from stable storage
        try:
            filename = self.grpaddr + '@' + self.address + '.dat'
        except AttributeError:
            filename = self.address + '.dat'
            
        try:
            self.__shelf = shelve.open(filename)
            self.__n = self.__shelf[Acceptor.PAXOS_ACCEPTOR_N]
            self.__h = self.__shelf[Acceptor.PAXOS_ACCEPTOR_H]
        except KeyError:
            self.__n = None
            self.__h = None
        except Exception as e:
            pass    # TODO: create an exception for this problem and raise it here
            
        self.__v = None
        self.__proposal_checker = proposal_checker
                
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
        self.__shelf[Acceptor.PAXOS_ACCEPTOR_N] = self.__n
        self.__shelf.sync()
        
    @h.setter
    def set_h ( self, h ):
        '''
        Increases h atomically
        '''
        #with lock_n:
        self.__h = h
        self.__shelf[Acceptor.PAXOS_ACCEPTOR_H] = self.__h
        self.__shelf.sync()
        
    @property
    def v ( self ):
        '''The value of the highest proposal accepted by this acceptor'''
        return self.__h

    @server.ProtocolAgent.handles('PrepareMsg')
    def handlePrepare (self, msg, src):
        # If we've already responded to a prepare request with a highest number
        # this request shall never end up in a successful accept
        if msg.n <= self.__h:
            logger.debug(
                "Acceptor at proposal %d ignoring proposal with number %d from proposer %s",
                self.__h, msg.n, src)
            # Should we tell the proposer if msg.n < self.__h so he can abandon the proposal?
            # Notice of that's the case for a number of acceptors, each acceptor would
            # generate one message to the proposer. Might limit scalability.
            return
        
        if msg.n <= self.__n or 0:
            logger.debug(
                "Acceptor having accepted %d ignoring proposal with number %d from proposer %s",
                self.__n, msg.n, src)
            return
        
        if self.__proposal_checker and self.__proposal_checker.check(msg.v):
            self.h = msg.n
            return type(self).PrepareRspMsg(msg.n, True, self.__n, self.__v)
        else:
            return type(self).PrepareRspMsg(msg.n, False, self.__n, self.__v)
            
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

Acceptor.shutdown = _shutdown


@server.ProtocolAgent.UDP
class Learner(object):
    '''
    This class implements the Paxos' learner role
    '''
    def __init__(self, params):
        '''
        Constructor
        '''
        pass
