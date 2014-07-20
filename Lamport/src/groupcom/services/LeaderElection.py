'''
Created on 13/03/2014

@author: ecejjar

A stable leader elector class following the algorithm in the seminal
paper by Aguilera et al:

"Intuitively, processes execute in rounds r = 0, 1, 2, . . . , where variable
r keeps the process’s current round. To start a round k, a process (1) sends (START , k)
to a specially designated process, called the “leader of round k”; this is just process
k mod n, (2) sets r to k, (3) sets the output of Ω to k mod n and (4) starts a timer —
a variable that is automatically incremented at each clock tick. While in round r, the
process checks if it is the leader of that round (task 0) and if so sends (OK , r) to all every
δ time.7 When a process receives an (OK , k) for the current round (r = k), the process
restarts its timer. If the process does not receive (OK , r) for more than 2δ time, it times
out on round r and starts round r + 1. If a process receives (OK , k) or (START , k)
from a higher round (k > r), the process starts that round.

Intuitively, this algorithm works because it guarantees that (1) if the leader of the
current round crashes then the process starts a new round and (2) processes eventually
reach a round whose leader is a correct process that sends timely (OK , k) messages."
'''

from collections import namedtuple
from threading import Timer, current_thread
from groupcom import server
import logging

logger = logging.getLogger(__name__)

@server.ProtocolAgent.UDP
class LeaderElector(object):
    '''
    Basic leader election as described by Aguilera et al.
    This algorithm is not stable, check unit tests.
    This algorithm can't deal with lossy links.
    '''
    StartMsg = namedtuple('StartMsg', 'round')
    OkMsg = namedtuple('OkMsg', 'round')
    
    @property
    def r ( self ):
        '''The current round as estimated by this process.'''
        return self.__round
    
    @property
    def n ( self ):
        '''The number of processes currently known.'''
        return len(self.__peers)
    
    @property
    def p ( self ):
        '''This process' index within the list of known processes.''' 
        try:
            p = self.__peers.index(self.server_address)
        except ValueError:
            p = self.n
            self.__peers.append(self.server_address)
        return p

    @property
    def leader ( self ):
        '''Tells whether this process believes it's the current leader.'''
        return self.__leader
    
    @property
    def timer0 ( self ):
        '''Timer driving task0 from the stable leader election algorithm'''
        return self.__task0
    
    @property
    def timer1 ( self ):
        'Timer driving task1 from the stable leader election algorithm'
        return self.__timer
    
    def restartTimer ( self ):
        'Restarts timer1'
        if self.__timer is not None:
            if current_thread() != self.__timer:
                # This happens when startRound() is called from task1; in this case timer1
                # has already elapsed and doesn't need to be cancelled. Calling cancel()
                # when the timer has elapsed is harmless but in this case we'd be calling
                # it from within the same Timer thread which may cause a deadlock.
                self.__timer.cancel()
        self.__timer = Timer(2*self.__timeout, self.__class__.task1, args=(self,))
        self.__timer.start()
        
    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor
        @peers List of participating processes (process addresses)
        @timeout Time between leadership checks, should be greater than D+2*SDEV(D)
        @observer An object that shall be notified when the current leader changes
        '''
        self.__timeout = timeout
        self.__peers = list(peers) or [self.server_address]
        self.__round = 0
        self.__leader = None
        self.__observer = observer
        self.__timer = None
        self.__task0 = server.RepeatableTimer(self.__timeout, self.__class__.task0, args=(self,))
        #...
        
    def startRound ( self, s ):
        '''
        Starts round s by sending a StartMsg with round s to process
        number s % n (only if s % n not equal to self process number).
        Sets current round to s and current leader to None.
        Notifies registered observer.
        Re-starts the timer governing task 1.
        @s Number of the round to start
        '''
        l = s % self.n
        logger.debug(\
            "%s: process %d in %d starting round %d with peer %d %s",
            self.__class__.__name__, self.p, self.n, s, l, str(self.__peers[l]) )
        if self.p != l:
            try:
                self.send(self.__class__.StartMsg(s), self.__peers[l])
            except:
                logger.error(\
                    "%s: process %d failed sending message on socket with sd %d",
                    self.__class__.__name__, self.p, self.socket.fileno() )
            #self.restartTimer1()
        self.__round = s
        self.__leader = l
        self.__observer and self.__observer.notify(self)
        self.restartTimer()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every self.__timeout seconds.
        '''
        if self.p == self.r % self.n:
            logger.debug(\
                "%s: leader process %d sending OK to %d peers",
                self.__class__.__name__, self.p, self.n )
            try:
                rcvrlist = map(lambda rcvr: self.send(self.__class__.OkMsg(self.r), rcvr), self.__peers)
            except:
                rcvrlist = ['dummy']
            if any(rcvrlist):
                logger.error("Peer %d failed sending OK to one or more peers")
            
    def task1 ( self ):
        '''
        It's been 2*self.__timeout without OKs from current leader - start new round
        '''
        logger.info(\
            "%s: process %d timed-out on round %d", self.__class__.__name__, self.p, self.r)
        self.startRound(self.r + 1)

    @server.ProtocolAgent.handles('StartMsg')        
    def handleStartMessage ( self, msg, src ):
        '''
        Handler for the Start message.
        If the message comes from an unknown process, add the sender's
        address to the list of known processes.
        If the message is calling for a round lower than this process'
        current round, just ignore it (delayed message).
        If the message is calling for a round higher than this process'
        current round, start the round called for the message (this
        behavior is in fact a Lamport clock, hence it causes a total
        ordering of rounds across all the processes).
        '''
        logger.debug(\
            "%s: process %d received Start message for round %d from peer at %s",
            self.__class__.__name__, self.p, msg.round, src )
        if src not in self.__peers: self.__peers.add(src)
        k = msg.round
        if k > self.r:
            self.startRound(k)
    
    @server.ProtocolAgent.handles('OkMsg')        
    def handleOkMessage ( self, msg, src ):
        '''
        Handler for the Ok message.
        If the message comes from an unknown process just ignore it.
        If the message is calling for a round lower than this process'
        current round, just ignore it (delayed message).
        If the message is calling for the same round as this process'
        current round, re-start task 1.
        If the message is calling for a round higher than this process'
        current round, start the round called for by the message.
        '''
        logger.debug(\
            "%s: process %d received Ok message for round %d from peer at %s",
            self.__class__.__name__, self.p, msg.round, src )
        if src in self.__peers:
            k = msg.round
            if k == self.r:
                self.restartTimer()
            elif k > self.r:
                self.startRound(k)
        else:
            logging.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)

def _serve_forever ( self ):
    '''
    Overloads the UDPServer.serve_forever() method adding task0 and task1 start
    before calling the method and stop before returning.
    '''
    self.startRound(0)  # startRound() calls self.timer1.start()
    self.timer0.start() # for safety, do not start task0 before having started the round
    
    super(self.__class__, self).serve_forever()
    
    self.timer0.cancel()
    self.timer1.cancel()
LeaderElector.serve_forever = _serve_forever


class StableLeaderElector(LeaderElector):
    '''
    Stable leader election as described by Aguilera et al.
    This algorithm is stable, check unit tests.
    This algorithm can't deal with lossy links.
    '''
    StopMsg = namedtuple('StopMsg', 'round')
    
    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor.
        Starts round 0, task 0 and task 1
        @peers Initial list of participating processes (process addresses), defaults to own address
        @timeout Time between leadership checks, should be greater than D+2*SDEV(D) (defaults to 200ms)
        @observer An object that shall be notified when the current leader changes (defaults to None)
        '''
        super(StableLeaderElector, self).__init__(self, peers, timeout, observer)
        #...
        
    def task1 ( self ):
        '''
        It's been 2*self.__timeout without OKs from current leader;
        send Stop to current leader and start new round
        '''
        # Can't use the leader property instead of r%n since if might be None
        self.send(self.__class__.StopMsg(self.r), self.__peers[self.r % self.n])
        super(StableLeaderElector, self).task1()

    @server.ProtocolAgent.handles('StopMsg')        
    def handleStopMessage ( self, msg, src ):
        '''
        Handler for the Stop message.
        If the message comes from an unknown process just ignore it.
        If the message is calling for a round NOT LOWER than the process'
        current round, start the next round.
        '''
        if src in self.__peers:
            k = msg.round
            if k >= self.r:
                self.startRound(k+1)
        
    def handleOkMessage ( self, msg, src ):
        '''
        Handler for the Ok message.
        If the message comes from an unknown process just ignore it.
        If the message is calling for a round lower than this process'
        current round, just ignore it (delayed message).
        If the message is calling for the same round as this process'
        current round, re-start task 1.
        If the message is calling for a round higher than this process'
        current round, start the round called for by the message.
        '''
        if src in self.__peers:
            k = msg.round
            if k == self.r:
                self.__okcount += 1
                if self.__okcount == 2 and self.leader is None:
                    self.__okcount = 0
                    self.__leader = k % self.n
                # super-class method calls self.restartTimer()
            elif k > self.r:
                self.__okcount = 0
                # super-class method calls self.startRound(k)
        super(StableLeaderElector, self).handleOkMessage(msg,src)
