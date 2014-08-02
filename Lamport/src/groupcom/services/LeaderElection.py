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
        if not hasattr(observer, 'notify') or not callable(observer.notify):
            raise ValueError("observer does not have a notify() method")
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
    def peers ( self ):
        return self.__peers
        
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
    
    The class estimates the network delay D and clock offset O to every peer
    sending an ack. It works as follows:
    
        C0(t1)                C0(t4)
    0 -+----------------------^------------------------------> t
        \                    /
         \ Msg(C0(t1))      / Ack(C0(t1),C1(t2),C1(t3))
          \                /
    1 -----V---------------+---------------------------------> t
            C1(t2)    C1(t3)
            
    Knowing C0(t1), C1(t2), C1(t3) and C0(t4), process 0 can estimate D and O as follows:
        
    Network transmission delay can be obtained as:
            [C0(t4) - C0(t1)] - [C1(t3) - C1(t2)]
        D = -------------------------------------
                            2
                            
    The peer clock offset can be obtained as:
            [C1(t2) - C0(t1)] + [C1(t3) - C0(t4)]
        O = -------------------------------------
                            2
    
    D is always positive since p1.receive(Ack) happens before p1.send(Msg).
    O is positive if process 1's clock is ahead of process 0's; otherwise it is negative.
    So C1(t) = C0(t) + O and C0(t) = C1(t) - O

    The class keeps running average and standard deviation for D and O to every peer.
    '''

    ''' Internal data structures used to hold info about a peer's offset with respect to us '''
    StatInfo = namedtuple('StatInfo', 'avg, stddev, n')        
    PeerInfo = namedtuple('PeerInfo', 'offset, delay')
    
    '''When there's no registered info about one peer, we provide this default data'''
    NO_INFO = PeerInfo(StatInfo(0,0,0), StatInfo(0,0,0))

    def __init__ ( self ):
        '''
        Constructor. Initialization of internal data structures.
        '''
        self.__peerinfo = {}

    def O (self, src):
        '''
        Accessor for clock offset estimations. Only the current leader has accurate estimations of
        offset at all peers; follower peers do only know accurately the current leader clock offset.
        @param src: tuple (address, port) containing transport address of the peer whose clock offset is sought 
        @return: current estimation of clock offset to peer
        '''
        return self.__peerinfo.get(src, type(self).NO_INFO).offset

    def D (self, src):
        '''
        Accessor for network delay estimations. Only the current leader has accurate estimations of
        delay to all peers; follower peers do only know accurately the delay to the current leader.
        @param src: tuple (address, port) containing transport address of the peer whose clock offset is sought 
        @return: current estimation of clock offset to peer
        '''
        return self.__peerinfo.get(src, type(self).NO_INFO).delay
    
    def processAckTimestamp ( self, ackmsg, src ):
        '''
        Ack messages carry the acked message timestamp C0(t1) and the
        peer's timestamps for message reception C1(t2) and ack sending C1(t3).
        C0(t4) is the current local time, at which we received the ack message.
        
        @param ackmsg: the message as received from the sending peer
        @param src: address&port tuple with the address of the sending peer
        '''
        try:
            # Obtain sample values from the received message and current time
            C0_t1 = float(ackmsg.msg_ts)
            C1_t2 = float(ackmsg.msg_rcv_ts)
            C1_t3 = float(ackmsg.timestamp)
            C0_t4 = time()
            D = ((C0_t4-C0_t1) - (C1_t3-C1_t2))/2
            O = ((C1_t2-C0_t1) + (C1_t3-C0_t4))/2
            
            # Obtain the current values
            offsetinfo, delayinfo = self.__peerinfo[src]
        except KeyError:
            offsetinfo, delayinfo = type(self).StatInfo(O, 0, 0), type(self).StatInfo(D, 0, 0)
        except AttributeError as e:
            logger.info("%s.processAckTimestamp() received message with missing field: %s", type(self).__name__, e)
            return
            
        # Update continuous estimate for peer offset
        avg, stddev, n = offsetinfo
        avg = (avg*n + O)/(n+1)
        stddev = (stddev*n + abs(O - avg))/(n+1)
        offsetinfo = type(self).StatInfo(avg, stddev, n+1)
        logger.debug("Offset of peer %s: avg = %f, stddev = %f", src, avg, stddev)
                
        # Update continuous estimate for peer network delay
        avg, stddev, n = delayinfo
        avg = (avg*n + D)/(n+1)
        stddev = (stddev*n + abs(D - avg))/(n+1)
        delayinfo = type(self).StatInfo(avg, stddev, n+1)
        logger.debug("Delay to peer %s: avg = %f, stddev = %f", src, avg, stddev)

        # Store the updated values
        self.__peerinfo[src] = type(self).PeerInfo(offsetinfo, delayinfo)
        
    def processOkTimestamp ( self, okmsg, src ):
        '''
        Ok messages received from the leader include his estimation for D and O,
        which is more recent than the estimation we may have (if we have any).
        Thus replace/add the leader estimation to our table.
        '''
        try:
            self.__peerinfo[src] = type(self).PeerInfo(okmsg.O, okmsg.D)
        except AttributeError as e:
            logger.warning(
                "%s.processOkTimestamp() received message with missing field: %s",
                type(self).__name__, e)            

    def discard ( self, msg, src ):
        '''
        Tells whether a message is to be discarded because it didn't fulfill the first
        condition of expiring links:
        
        "(No late delivery): If p sends m to q by time t − δ then q does not receive m after t"
        
        Implementation notes:
        A threshold of 3 times the estimated stddev is allowed before discarding the message.
        When less than 10 stddev samples are available, twice the average is allowed instead.
         
        @param msg: the message to be analyzed
        @param src: tuple (address, port) for the message sender's source address
        @return: True if the message failed to fulfill the 1st condition, false otherwise
        '''
        thrsh = 3                               # Allow 3 stddev to compensate for estimate error
        self_time = time()
        try:
            offsetinfo, delayinfo = self.__peerinfo[src]
            avg, stddev, n = offsetinfo
            if n == 0:
                raise KeyError("Leader has not received any Ack to its Ok messages yet")
            elif n < 10:
                stddev = avg/3                  # Allow 2x threshold when stddev info not reliable
            if avg < 0: thrsh = -thrsh          # O/w stddev wouldn't add but substract from avg when avg<0
            msg_delay = self_time - msg.timestamp + (avg + thrsh*stddev)
            logger.debug("Estimated message delay for peer %s = %f", src, msg_delay)
            return msg_delay > self.d
        except KeyError:
            logger.debug("No data about peer %s, letting the message get by", src)
            return False


@server.ProtocolAgent.UDP
class OnStableLeaderElector(ExpiringLinksImpl):
    '''
    O(n) stable leader election with lossy links as described by Aguilera et al.
    Handles message losses using the expiring links implementation it extends.
    The maximum leader election time is (n+4)d (n/2+2 time-outs).
    
    Some notes about measuring message delay:
    The ExpiringLinksImpl this class extends uses message/ack exchanges to
    measure clock offset and network delay to a remote peer.
    To use that implementation efficiently, the current leader measures
    offset and delay to every peer using the ack messages the followers send
    after receiving ok from the leader; then it distributes offset and delay
    to every peer using the ok messages it sends.
    Every peer then uses the known values of offset and delay to estimate
    the network delay of each message received, and if that delay exceeds
    the maximum delay allowed d then it discards the message.

    Message exchange takes place as follows (L = leader, F = follower):
    
    L              F1             F2             F3
    | Start(tF1,L) |              |              |
    |<-------------| Start(tF2,L) |              |
    |<----------------------------| Start(tF3,L) |
    |  OK(tL,0,L)  |              |       X------|
    |------------->| OK(tL,0,L)   |              |
    |---------------------------->|  OK(tL,0,L)  |
    |------------------------------------------->|
    |              |              |              + (No valid data so message is accepted)
    | Ack(tF1,tL,tF1',L)          |              |
    |<-------------| Ack(tF2,tL,tF2',L)          |
    |<----------------------------| Ack(tF3,tL,tF3',L)
    |<-------------------------------------------|
    |              |              |              |
    + (Calculate peer data)       |              |
    |              |              |              |
    |  OK(tL',D,L) |              |              |
    |------------->| OK(tL',D,L)  |              |
    |---------------------------->|  OK(tL',D,L) |
    |------------------------------------------->|
    |              |              |              + (Less than 10 samples so twice D delay is allowed)
    |              |              |              |
    '''
    
    StartMsg = namedtuple('StartMsg', 'timestamp, round')
    OkMsg = namedtuple('OkMsg', 'timestamp, O, D, round')
    AckMsg = namedtuple('AckMsg', 'timestamp, msg_ts, msg_rcv_ts, round')

    # TODO: once this works, remove common code by extending LeaderElectorBase    
    def __init__ ( self, peers = [], timeout = 0.2, ackratio = 0.1, observer = None ):
        '''
        Constructor
        @param peers: List of participating processes (process addresses)
        @param timeout: Time for declaring a leader dead, should be greater than D+2*SDEV(D) (D=2d)
        @param ackratio: percentage of Ok messages this peer shall acknowledge with an Ack message
        @param observer: An object that shall be notified when the current leader changes
        '''
        ExpiringLinksImpl.__init__(self)
        self.__timeout = timeout
        if ackratio <= 0 or ackratio >= 1:
            raise ValueError("ackratio must be greater than 0 and lower than 1")
        self.__ackratio = ackratio
        self.__peers = list(peers) or [self.server_address]
        self.__round = 0
        self.__leader = None
        if not hasattr(observer, 'notify') or not callable(observer.notify):
            raise ValueError("observer does not have a notify() method")
        self.__observer = observer
        self.__timer = None
        self.__task0 = server.RepeatableTimer(self.__timeout/2, type(self).task0, args=(self,))
        self.__okcount = 0
        self.__okslefttoack = 1 # This causes the first Ok to be ack'ed
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
        'Restarts timer governing task 1'
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
        if s < 0: raise ValueError("s must be greater or equal 0")
        s = int(s)
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
            rcvrlist = map(
                lambda rcvr: self.send(type(self).OkMsg(time(), self.O(rcvr), self.D(rcvr), self.r), rcvr),
                self.__peers)
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

    def sendAckIfNeeded ( self, msg_rcv_ts, msg, src ):
        '''
        Checks if an Ack is to be sent to the current leader
        '''
        self.__okslefttoack -= 1
        if not self.__okslefttoack:
            self.__okslefttoack = 1 // self.__ackratio
            self.send(type(self).AckMsg(time(), msg.timestamp, msg_rcv_ts, self.r), src)

    @server.ProtocolAgent.handles('StartMsg')        
    def handleStartMessage ( self, msg, src ):
        '''
        Handler for the Start message.
        If the message comes from an unknown process, add the sender's
        address to the list of known processes.
        If the message is calling for a round lower than this process'
        current round, send Start to the peer (it may have missed some Ok messages).
        If the message is calling for a round higher than this process'
        current round, start the round called for the message (this
        behavior is in fact a Lamport clock, hence it causes a total
        ordering of rounds across all the processes).
        '''
        logger.debug(\
            "%s: process %d received Start message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )

        if self.discard(msg, src):
            logger.debug("Discarding Start message from peer %s with timestamp %f", src, msg.timestamp)
            return
        
        if src not in self.__peers: self.__peers.add(src)
        k = msg.round
        if k > self.r:
            self.startRound(k)
        elif k < self.r:
            self.send(type(self).StartMessage(time(), self.r), src)
            #return type(self).StartMessage(time(), self.r) should work
                         
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
        
        # Log the message reception time
        msg_rcv_ts = time()
        
        logger.debug(\
            "%s: process %d received Ok message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        # Consider the latest leader estimation before deciding if discard a message
        self.processOkTimestamp(msg, src)
        
        if self.discard(msg, src):
            logger.debug("Discarding Ok message from peer %s with timestamp %f", src, msg.timestamp)
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
            self.send(type(self).StartMsg(time(), self.r), src)
        
        # Tell the leader about our timings
        self.sendAckIfNeeded(msg_rcv_ts, msg, src)

    @server.ProtocolAgent.handles('AckMsg')        
    def handleAckMessage ( self, msg, src ):
        '''
        Handler for the Ack message.
        Delegates calculation of clock offset and network delay to ExpiringLinksImpl base class.
        '''
        logger.debug(\
            "%s: process %d received Ack message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )

        if src not in self.__peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        # Acks from known peers are never discarded, they carry useful info
        self.processAckTimestamp(msg, src)

OnStableLeaderElector.serve_forever = _serve_forever


@server.ProtocolAgent.UDP
class O1StableLeaderElector(LeaderElectorBase, ExpiringLinksImpl):
    '''
    O(1) stable leader election with lossy links as described by Aguilera et al.
    Handles message losses using the expiring links implementation it extends.
    The maximum leader election time is 6d (3 time-outs).
    '''
    StartMsg = namedtuple('StartMsg', 'timestamp, round')
    OkMsg = namedtuple('OkMsg', 'timestamp, O, D, round')
    AlertMsg = namedtuple('AlertMsg', 'timestamp, round')
    AckMsg = namedtuple('AckMsg', 'timestamp, msg_ts, msg_rcv_ts, round')

    '''Stores round and local time of the AlertMsg with the highest round value received''' 
    LastAlertInfo = namedtuple("LastAlert", "round, time")
    
    def __init__ ( self, peers = [], timeout = 0.2, ackratio =0.1, observer = None ):
        '''
        Constructor
        @param peers: List of participating processes (process addresses)
        @param timeout: Time between leadership checks, should be greater than D+2*SDEV(D)
        @param observer: An object that shall be notified when the current leader changes
        '''
        LeaderElectorBase.__init__(self, peers, timeout, observer)
        ExpiringLinksImpl.__init__(self)
        if ackratio <= 0 or ackratio >= 1:
            raise ValueError("ackratio must be greater than 0 and lower than 1")
        self.__ackratio = ackratio
        self.__okcount = 0
        self.__okslefttoack = 1 # This causes the first Ok to be ack'ed
        self.__lastalert = type(self).LastAlertInfo(0, 0) 
        #...
        
    def startRound ( self, s ):
        '''
        Starts round s by sending a StartMsg with round s to all
        processes (only if s % n not equal to self process number).
        Sets current round to s and current leader to None.
        Notifies registered observer.
        Re-starts the timer governing task 1.
        @param s: Number of the round to start
        '''
        if s < 0: raise ValueError("s must be greater or equal 0")
        l = s % self.n
        logger.debug(
            "%s: process %d in %d starting round %d with peer %d %s",
            type(self).__name__, self.p, self.n, s, l, self.peers[l] )
        if self.p != l:
            rcvrlist = map(lambda rcvr: self.send(type(self).StartMsg(time(), s), rcvr), self.peers)
            try:
                if any(rcvrlist):
                    logger.error("Peer %d failed sending OK to one or more peers")
            except Exception as e:
                logger.error("Peer %d failed sending OK to one or more peers, error: ", e)
        self.__round = s
        self.leader = None
        self.restartTimer()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every self.__timeout seconds.
        '''
        if self.p == self.r % self.n:
            logger.debug(
                "%s: leader process %d sending OK to %d peers",
                type(self).__name__, self.p, self.n )
            rcvrlist = map(
                lambda rcvr: self.send(type(self).OkMsg(time(), self.O(rcvr), self.D(rcvr), self.r), rcvr),
                self.peers)
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

    def sendAckIfNeeded ( self, msg_rcv_ts, msg, src ):
        '''
        Checks if an Ack is to be sent to the current leader
        '''
        self.__okslefttoack -= 1
        if not self.__okslefttoack:
            self.__okslefttoack = 1 // self.__ackratio
            self.send(type(self).AckMsg(time(), msg.timestamp, msg_rcv_ts, self.r), src)

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

        if self.discard(msg, src):
            logger.debug("Discarding Start message from peer %s with timestamp %f", src, msg.timestamp)
            return
        
        if src not in self.peers: self.peers.add(src)
        k = msg.round
        if k > self.r:
            self.startRound(k)
        elif k < self.r:
            self.send(type(self).StartMessage(time(), self.r), src)
            #return type(self).StartMessage(time(), self.r) should work
                        
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
        
        # Log the message reception time
        msg_rcv_ts = time()
        
        logger.debug(\
            "%s: process %d received Ok message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )
        
        if src not in self.peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        # Consider the latest leader estimation before deciding if discard a message
        self.processOkTimestamp(msg, src)
        
        if self.discard(msg, src):
            logger.debug("Discarding Ok message from peer %s with timestamp %f", src, msg.timestamp)
            return
        
        k = msg.round
        if k == self.r:
            self.__okcount += 1
            if self.leader is None and self.__okcount >= 2 and \
              ( time() - self.__lastalert.time > 6*self.d or self.__lastalert.round <= k):
                self.__okcount = 0
                self.leader = k % self.n
            self.restartTimer()
        elif k > self.r:
            self.__okcount = 0
            self.startRound(k)
        else: # hence k < self.r
            self.send(type(self).StartMessage(time(), self.r), src)
        
        # Tell the leader about our timings
        self.sendAckIfNeeded(msg_rcv_ts, msg, src)

    @server.ProtocolAgent.handles('AlertMsg')
    def handleAlertMsg ( self, msg, src ):
        logger.debug(\
            "%s: process %d received Alert message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )

        if self.discard(msg, src):
            logger.debug("Discarding Alert message from peer %s with timestamp %f", src, msg.timestamp)
            return
        
        if src not in self.peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        k = msg.round
        if k > self.r:
            self.leader = None
            
        # In tuple comparison, the element with the lowest index weighs the most
        self.__lastalert = max(type(self).LastAlertInfo(k, time()), self.__lastalert)

    @server.ProtocolAgent.handles('AckMsg')        
    def handleAckMessage ( self, msg, src ):
        '''
        Handler for the Ack message.
        Delegates calculation of clock offset and network delay to ExpiringLinksImpl base class.
        '''
        logger.debug(\
            "%s: process %d received Ack message for round %d from peer at %s",
            type(self).__name__, self.p, msg.round, src )

        if src not in self.peers:
            logger.warning("Peer %d received message %s from unknown peer %s", self.p, msg, src)
            return

        # Acks from known peers are never discarded, they carry useful info
        self.processAckTimestamp(msg, src)
        
O1StableLeaderElector.serve_forever = _serve_forever


@server.ProtocolAgent.UDP
class ConstantElectionTimeStableLeaderElector(LeaderElectorBase):
    pass

class EventuallyPerfectFailureDetector:
    pass

