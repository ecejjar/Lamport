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
from time import time
from groupcom import server
import logging
import socket

logger = logging.getLogger(__name__)

def _serve_forever ( self ):
    '''
    Overloads the UDPServer.serve_forever() method adding task0 and task1 start
    before calling the method and stop before returning.
    '''
    self.startRound(0)  # startRound() calls self.timer1.start()
    self.timer0.start() # for safety, do not start task0 before having started the round
    
    super(type(self), self).serve_forever()
    
    self.timer0.cancel()
    self.timer1.cancel()


class LeaderElectorBase(object):
    '''
    This class provides boiler-plate code for all the leader elector implementations
    in the module.
    '''

    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor
        @peers List of participating processes (process addresses)
        @timeout Time for declaring a leader dead, should be greater than D+2*SDEV(D) (D=2d)
        @observer An object that shall be notified when the current leader changes
        '''
        self.__timeout = timeout
        self.__peers = list(peers) or [self.server_address]
        self.__round = 0
        self.__leader = None
        self.__observer = observer
        self.__timer = None
        self.__task0 = server.RepeatableTimer(self.__timeout/2, type(self).task0, args=(self,))
        #...
        
    @property
    def r ( self ):
        '''The current round as estimated by this process.'''
        return self.__round

    @property
    def d ( self ):
        '''
        The assumed value of d (maximum time it takes for a link to transfer a message).
        This value comes determined by the time-out between leadership checks passed
        to the constructor; more precisely, it is exactly half that time-out.
        '''
        return self.__timeout / 2
        
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
    
    @leader.setter
    def leader ( self, p ):
        '''Sets the leader to the value passed as argument and notifies the registered observer, if any'''
        self.__leader = p
        try:
            self.__observer and self.__observer.notify(self)
        except Exception as e:
            logger.warning(
                "Exception in observer when process %d notified its current leader is %s: %s",
                self.p, p, e)
        
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
        self.__timer = Timer(self.__timeout, type(self).task1, args=(self,))
        self.__timer.start()
    
    def commonStartMessageHandling ( self, msg, src ):
        logger.debug(\
            "%s: process %d received Start message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )
        if src not in self.__peers: self.__peers.add(src)
    
    def commonOkMessageHandling ( self, msg, src ):
        logger.debug(\
            "%s: process %d received Ok message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return False
        
        return True
    
@server.ProtocolAgent.UDP
class LeaderElector(object):
    '''
    Basic leader election as described by Aguilera et al.
    This algorithm is not stable, check unit tests.
    This algorithm can't deal with lossy links.
    '''

    # TODO: once this works, remove common code by extending LeaderElectorBase    
    StartMsg = namedtuple('StartMsg', 'round')
    OkMsg = namedtuple('OkMsg', 'round')
    
    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor
        @peers List of participating processes (process addresses)
        @timeout Time for declaring a leader dead, should be greater than D+2*SDEV(D) (D=2d)
        @observer An object that shall be notified when the current leader changes
        '''
        self.__timeout = timeout
        self.__peers = list(peers) or [self.server_address]
        self.__round = 0
        self.__leader = None
        self.__observer = observer
        self.__timer = None
        self.__task0 = server.RepeatableTimer(self.__timeout/2, type(self).task0, args=(self,))
        #...
        
    @property
    def r ( self ):
        '''The current round as estimated by this process.'''
        return self.__round

    @property
    def d ( self ):
        '''
        The assumed value of d (maximum time it takes for a link to transfer a message).
        This value comes determined by the time-out between leadership checks passed
        to the constructor; more precisely, it is exactly half that time-out.
        '''
        return self.__timeout / 2
        
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
        self.__timer = Timer(self.__timeout, type(self).task1, args=(self,))
        self.__timer.start()
        
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
            type(self).__name__, self.p, self.n, s, l, str(self.__peers[l]) )
        if self.p != l:
            try:
                if self.send(type(self).StartMsg(s), self.__peers[l]):
                    raise socket.error("not all data sent")
            except Exception as e:
                logger.error(\
                    "%s: process %d failed sending start message to peer %d at %s, error: %s",
                    type(self).__name__, self.p, l, self.__peers[l], e )
                raise e # This algorithm doesn't tolerate message losses
        self.__round = s
        self.__leader = l
        self.__observer and self.__observer.notify(self)
        self.restartTimer()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every d seconds.
        '''
        if self.p == self.r % self.n:
            logger.debug(\
                "%s: leader process %d sending OK to %d peers",
                type(self).__name__, self.p, self.n )
            rcvrlist = map(lambda rcvr: self.send(type(self).OkMsg(self.r), rcvr), self.__peers)
            try:
                if any(rcvrlist):
                    raise socket.error("not all data sent")
            except Exception as e:
                logger.error("Peer %d failed sending OK message to one or more peers, error: %s", self.p, e)
                raise e # This algorithm doesn't tolerate message losses
            
    def task1 ( self ):
        '''
        It's been 2d seconds without OKs from current leader - start new round
        '''
        logger.info(\
            "%s: process %d timed-out on round %d", type(self).__name__, self.p, self.r)
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
            type(self).__name__, self.p, msg.round, src )
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
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)

        k = msg.round
        if k == self.r:
            self.restartTimer()
        elif k > self.r:
            self.startRound(k)

LeaderElector.serve_forever = _serve_forever


@server.ProtocolAgent.UDP
class StableLeaderElector(object):
    '''
    Stable leader election as described by Aguilera et al.
    This algorithm is stable, check unit tests.
    This algorithm can't deal with lossy links.
    '''
    
    # TODO: once this works, remove common code by extending LeaderElectorBase    
    StartMsg = namedtuple('StartMsg', 'round')
    OkMsg = namedtuple('OkMsg', 'round')    
    StopMsg = namedtuple('StopMsg', 'round')
    
    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor
        @peers List of participating processes (process addresses)
        @timeout Time for declaring a leader dead, should be greater than D+2*SDEV(D) (D=2d)
        @observer An object that shall be notified when the current leader changes
        '''
        self.__timeout = timeout
        self.__peers = list(peers) or [self.server_address]
        self.__round = 0
        self.__leader = None
        self.__observer = observer
        self.__timer = None
        self.__task0 = server.RepeatableTimer(self.__timeout/2, type(self).task0, args=(self,))
        self.__okcount = 0
        #...
        
    @property
    def r ( self ):
        '''The current round as estimated by this process.'''
        return self.__round

    @property
    def d ( self ):
        '''
        The assumed value of d (maximum time it takes for a link to transfer a message).
        This value comes determined by the time-out between leadership checks passed
        to the constructor; more precisely, it is exactly half that time-out.
        '''
        return self.__timeout / 2
        
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
        self.__timer = Timer(self.__timeout, type(self).task1, args=(self,))
        self.__timer.start()
        
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
            "%s: process %d in %d starting round %d with peer %d at %s",
            type(self).__name__, self.p, self.n, s, l, self.__peers[l] )
        if self.p != l:
            try:
                if self.send(type(self).StartMsg(s), self.__peers[l]):
                    raise socket.error("not all data sent")
            except Exception as e:
                logger.error(\
                    "%s: process %d failed sending start message to peer %d at %s, error: %s",
                    type(self).__name__, self.p, l, self.__peers[l], e )
                raise e # this algorithm doesn't tolerate message losses
        self.__round = s
        self.__leader = None
        self.__observer and self.__observer.notify(self)
        self.restartTimer()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every d seconds.
        '''
        if self.p == self.r % self.n:
            logger.debug(\
                "%s: leader process %d sending OK to %d peers",
                type(self).__name__, self.p, self.n )
            rcvrlist = map(lambda rcvr: self.send(type(self).OkMsg(self.r), rcvr), self.__peers)
            try:
                if any(rcvrlist):
                    raise socket.error("not all data sent")
            except Exception as e:
                logger.error("Peer %d failed sending OK to one or more peers, error: %s", self.p, e)
                raise e # this algorithm doesn't tolerate message losses

    def task1 ( self ):
        '''
        It's been 2d without OKs from current leader;
        send Stop to current leader and start new round
        '''
        # Can't use the leader property instead of r%n since if might be None
        logger.info(\
            "%s: process %d timed-out on round %d", type(self).__name__, self.p, self.r)
        self.send(type(self).StopMsg(self.r), self.__peers[self.r % self.n])
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
            type(self).__name__, self.p, msg.round, src )
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
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return
            
        k = msg.round
        if k == self.r:
            self.__okcount += 1
            if self.leader is None and self.__okcount == 2:
                self.__okcount = 0
                self.__leader = k % self.n
                self.__observer and self.__observer.notify(self)
            self.restartTimer()
        elif k > self.r:
            self.__okcount = 0
            self.startRound(k)

    @server.ProtocolAgent.handles('StopMsg')        
    def handleStopMessage ( self, msg, src ):
        '''
        Handler for the Stop message.
        If the message comes from an unknown process just ignore it.
        If the message is calling for a round NOT LOWER than the process'
        current round, start the next round.
        '''
        logger.debug(\
            "%s: process %d received Stop message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return
        
        k = msg.round
        if k >= self.r:
            self.startRound(k+1)

StableLeaderElector.serve_forever = _serve_forever
        

class ExpiringLinksImpl(object):
    '''
    Supports expiring links by discarding messages taking longer than d to arrive.
    To achieve that, we assume that all clocks have a similar drift e, which is
    negligible compared to max network delay d (e << d).
    '''

    ''' Internal data structure used to hold info about a peer delta with respect to us '''        
    PeerDeltaInfo = namedtuple('PeerDeltaInfo', 'avg, n')
    
    def __init__ ( self ):
        self.__peerdeltainfo = {}
    
    def estimatePeerDelta ( self, timestamp, src ):
        # Estimate latest peer delta as the difference between clocks minus the maximum delay d
        self_time = time()
        total_delta = self_time - timestamp
        peer_delta = total_delta - self.d   # this is for sure lower or equal than actual delta
        
        # Update the continuous estimate
        try:
            avg_peer_delta, n_samples = self.__peerdeltainfo[src]
        except KeyError:
            avg_peer_delta, n_samples = peer_delta, 1
        avg_peer_delta = (avg_peer_delta*n_samples + peer_delta)/(n_samples+1)
        self.__peerdeltainfo[src] = type(self).PeerDeltaInfo(avg_peer_delta, n_samples+1)
        logger.debug("Estimated delta with peer %s = %f", src, avg_peer_delta)
        
    def discard ( self, timestamp, src ):
        self_time = time()
        peer_delta_info = self.__peerdeltainfo[src]
        return abs(self_time - timestamp - peer_delta_info.avg) > self.d 
        
    def handleMessageTimestamp ( self, msg, src ):
        timestamp = float(msg.timestamp)
        if self.discard(timestamp, src):
            logger.debug("Discarding Start message from peer %s with timestamp %f", src, timestamp)
            return True
        self.estimatePeerDelta(timestamp, src)
        return False
    

@server.ProtocolAgent.UDP
class OnStableLeaderElector(ExpiringLinksImpl):
    '''
    O(n) stable leader election with lossy links as described by Aguilera et al.
    Handles message losses using the expiring links implementation it extends.
    The maximum leader election time is (n+4)d (n/2+2 time-outs).
    '''
    
    # TODO: once this works, remove common code by extending LeaderElectorBase    
    StartMsg = namedtuple('StartMsg', 'timestamp, round')
    OkMsg = namedtuple('OkMsg', 'timestamp, round')

    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor
        @peers List of participating processes (process addresses)
        @timeout Time for declaring a leader dead, should be greater than D+2*SDEV(D) (D=2d)
        @observer An object that shall be notified when the current leader changes
        '''
        self.__timeout = timeout
        self.__peers = list(peers) or [self.server_address]
        self.__round = 0
        self.__leader = None
        self.__observer = observer
        self.__timer = None
        self.__task0 = server.RepeatableTimer(self.__timeout/2, type(self).task0, args=(self,))
        self.__okcount = 0
        #...
        
    @property
    def r ( self ):
        '''The current round as estimated by this process.'''
        return self.__round

    @property
    def d ( self ):
        '''
        The assumed value of d (maximum time it takes for a link to transfer a message).
        This value comes determined by the time-out between leadership checks passed
        to the constructor; more precisely, it is exactly half that time-out.
        '''
        return self.__timeout / 2
        
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
        self.__timer = Timer(self.__timeout, type(self).task1, args=(self,))
        self.__timer.start()
        
    def startRound ( self, s ):
        '''
        Starts round s by sending a StartMsg with round s to all
        processes (only if s % n not equal to self process number).
        Sets current round to s and current leader to None.
        Notifies registered observer.
        Re-starts the timer governing task 1.
        @s Number of the round to start
        '''
        l = s % self.n
        logger.debug(\
            "%s: process %d in %d starting round %d with peer %d at %s",
            type(self).__name__, self.p, self.n, s, l, str(self.__peers[l]) )
        if self.p != l:
            rcvrlist = map(lambda rcvr: self.send(type(self).StartMsg(time(), s), rcvr), self.__peers)
            try:
                if any(rcvrlist):
                    raise socket.error("not all data sent")
            except Exception as e:
                logger.warning("Peer %d failed sending OK to one or more peers, error: %s", self.p, e)
        self.__round = s
        self.__leader = None
        self.__observer and self.__observer.notify(self)
        self.restartTimer()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every self.__timeout seconds.
        '''
        if self.p == self.r % self.n:
            logger.debug(\
                "%s: leader process %d sending OK to %d peers",
                type(self).__name__, self.p, self.n )
            rcvrlist = map(lambda rcvr: self.send(type(self).OkMsg(time(), self.r), rcvr), self.__peers)
            try:
                if any(rcvrlist):
                    raise socket.error("not all data sent")
            except Exception as e:
                logger.warning("Peer %d failed sending OK to one or more peers, error: %s", self.p, e)
                
    def task1 ( self ):
        '''
        It's been 2*self.__timeout without OKs from current leader;
        send Stop to current leader and start new round
        '''
        # Can't use the leader property instead of r%n since if might be None
        logger.info(\
            "%s: process %d timed-out on round %d", type(self).__name__, self.p, self.r)
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
            type(self).__name__, self.p, msg.round, src )

        if self.handleMessageTimestamp(msg, src):
            return
        
        if src not in self.__peers: self.__peers.add(src)
        k = msg.round
        if k > self.r:
            self.startRound(k)
        elif k < self.r:
            self.send(type(self).StartMessage(time(), self.r), src)
                        
    @server.ProtocolAgent.handles('OkMsg')        
    def handleOkMessage ( self, msg, src ):
        '''
        Handler for the Ok message.
        If the message comes from an unknown process just ignore it.
        If the message is calling for a round lower than this process'
        current round, send start to the message originator (it missed
        the start/ok message(s) for the current round so needs a heads-up).
        If the message is calling for the same round as this process'
        current round, re-start task 1.
        If the message is calling for a round higher than this process'
        current round, start the round called for by the message.
        '''
        logger.debug(\
            "%s: process %d received Ok message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        if self.handleMessageTimestamp(msg, src):
            return
        
        k = msg.round
        if k == self.r:
            self.__okcount += 1
            if self.__okcount == 2 and self.leader is None:
                self.__okcount = 0
                self.__leader = k % self.n
                self.__observer and self.__observer.notify(self)
            self.restartTimer()
        elif k > self.r:
            self.__okcount = 0
            self.startRound(k)
        else: # hence k < self.r
            self.send(type(self).StartMessage(time(), self.r), src)

OnStableLeaderElector.serve_forever = _serve_forever


@server.ProtocolAgent.UDP
class O1StableLeaderElector(LeaderElectorBase, ExpiringLinksImpl):
    '''
    O(1) stable leader election with lossy links as described by Aguilera et al.
    Handles message losses using the expiring links implementation it extends.
    The maximum leader election time is 6d (3 time-outs).
    '''
    StartMsg = namedtuple('StartMsg', 'timestamp, round')
    OkMsg = namedtuple('OkMsg', 'timestamp, round')
    AlertMsg = namedtuple('AlertMsg', 'timestamp, round')

    '''Stores round and local time of the AlertMsg with the highest round value received''' 
    LastAlertInfo = namedtuple("LastAlert", "round, time")
    
    def __init__ ( self, peers = [], timeout = 0.2, observer = None ):
        '''
        Constructor
        @peers List of participating processes (process addresses)
        @timeout Time between leadership checks, should be greater than D+2*SDEV(D)
        @observer An object that shall be notified when the current leader changes
        '''
        super(O1StableLeaderElector, self).__init__(peers, timeout, observer)
        self.__okcount = 0
        self.__lastalert = O1StableLeaderElector.LastAlertInfo(0, 0) 
        #...
        
    def startRound ( self, s ):
        '''
        Starts round s by sending a StartMsg with round s to all
        processes (only if s % n not equal to self process number).
        Sets current round to s and current leader to None.
        Notifies registered observer.
        Re-starts the timer governing task 1.
        @s Number of the round to start
        '''
        l = s % self.n
        logger.debug(\
            "%s: process %d in %d starting round %d with peer %d %s",
            type(self).__name__, self.p, self.n, s, l, str(self.__peers[l]) )
        if self.p != l:
            rcvrlist = map(lambda rcvr: self.send(type(self).StartMsg(time(), s), rcvr), self.__peers)
            try:
                if any(rcvrlist):
                    logger.error("Peer %d failed sending OK to one or more peers")
            except Exception as e:
                logger.error("Peer %d failed sending OK to one or more peers, error: ", e)
            #self.restartTimer1()
        self.__round = s
        self.leader = None
        self.restartTimer()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every self.__timeout seconds.
        '''
        if self.p == self.r % self.n:
            logger.debug(\
                "%s: leader process %d sending OK to %d peers",
                type(self).__name__, self.p, self.n )
            rcvrlist = map(lambda rcvr: self.send(type(self).OkMsg(time(), self.r), rcvr), self.__peers)
            try:
                if any(rcvrlist):
                    logger.error("Peer %d failed sending OK to one or more peers")
            except Exception as e:
                logger.error("Peer %d failed sending OK to one or more peers, error: ", e)
                
    def task1 ( self ):
        '''
        It's been 2*self.__timeout without OKs from current leader;
        send Stop to current leader and start new round
        '''
        # Can't use the leader property instead of r%n since if might be None
        logger.info(\
            "%s: process %d timed-out on round %d", type(self).__name__, self.p, self.r)
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
        self.commonStartMessageHandling(msg, src)
        k = msg.round
        if k > self.r:
            self.startRound(k)
        elif k < self.r:
            self.send(type(self).StartMessage(self.r), src)
                        
    @server.ProtocolAgent.handles('OkMsg')        
    def handleOkMessage ( self, msg, src ):
        '''
        Handler for the Ok message.
        If the message comes from an unknown process just ignore it.
        If the message is calling for a round lower than this process'
        current round, send start to the message originator (it missed
        the start/ok message(s) for the current round so needs a heads-up).
        If the message is calling for the same round as this process'
        current round, re-start task 1.
        If the message is calling for a round higher than this process'
        current round, start the round called for by the message.
        '''
        if not self.commonOkMessageHandling(msg, src):
            return

        if self.handleMessageTimestamp(self, msg, src):
            return
        
        k = msg.round
        if k == self.r:
            self.__okcount += 1
            if self.leader is None and self.__okcount >= 2 \
              and self.__lastalert.round > k and time() - self.__lastalert.time > 6*self.__timeout :
                self.__okcount = 0
                self.leader = k % self.n
            self.restartTimer()
        elif k > self.r:
            self.__okcount = 0
            self.startRound(k)
        else: # hence k < self.r
            self.send(type(self).StartMessage(time(), self.r), src)

    @server.ProtocolAgent.handles('AlertMsg')
    def handleAlertMsg ( self, msg, src ):
        logger.debug(\
            "%s: process %d received Alert message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )

        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        if self.handleMessageTimestamp(self, msg, src):
            return
        
        k = msg.round
        if k > self.r:
            self.leader = None
            
        # In tuple comparison, the element with the lowest index weighs the most
        self.__lastalert = max(O1StableLeaderElector.LastAlertInfo(k, time()), self.__lastalert)

O1StableLeaderElector.serve_forever = _serve_forever


@server.ProtocolAgent.UDP
class ConstantElectionTimeStableLeaderElector(LeaderElectorBase):
    pass

class EventuallyPerfectFailureDetector:
    pass

