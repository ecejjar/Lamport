'''
Created on 13/02/2013

@author: ECEJJAR

A library of group communication servers, including reliable multicast servers
and Lamport clocks-based total ordering multicast servers
'''
try:
    from socketserver import ThreadingMixIn, TCPServer, UDPServer, BaseRequestHandler # Python 3.x
    from functools import reduce                                                      # Python 3.x
except:
    from SocketServer import ThreadingMixIn, TCPServer, UDPServer, BaseRequestHandler # Python 2.x

from functools import wraps    
from collections import namedtuple, deque
from struct import pack, unpack, calcsize
from heapq import heappush, heappop
from random import uniform
from threading import Timer, Lock, Thread, _Timer
from operator import add
from select import select
from time import clock
import shelve
import socket
import json
import os
import re
import platform
import logging

logger = logging.getLogger(__name__)

class McastServer(UDPServer):
    '''
    A multicast server (sender and receiver).
    Usage:
    >>> from groupcom.server import McastServer
    >>> import socket
    >>> host = socket.gethostbyname(socket.gethostname())
    >>> my_server = McastServer(('myhost', 2000), ('225.0.0.1', 2000), 32, MyHandlerClass)
    >>> server.serve_forever()
    '''

    def __init__ ( self, mcast_hostport, hostport=None, handler=None, ttl=32 ):
        '''
        Constructor
        '''
        self.__mcast_hostport = mcast_hostport
        self.__ttl = ttl
        if hostport is None:
            hostport = socket.gethostbyname(socket.gethostname())
        super(McastServer, self).__init__(hostport, handler, handler is not None)
        
    @property
    def grpaddr ( self ):
        return self.__mcast_hostport
    
    def server_bind ( self ):
        '''
        Overloads the base UDPServer.server_bind() method to set the necessary socket options
        that properly enable multicast sending and receiving.
        '''
        # Don't need this, it's done by allow_reuse_address = True
        #self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.allow_reuse_address = True
        
        # These options are required for sending only
        self.socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, self.__ttl)
        self.socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)

        # Call the superclass' server_bind(). On Windows, binding must be to the local address
        # while on Linux binding must be to the multicast address; thus, in the latter case we
        # overwrite self.server_address before calling super().server_bind() and restore it to
        # its previous value before returning.
        local_address = self.server_address
        if platform.system() == 'Linux':
                self.server_address = self.__mcast_hostport
                
        super(McastServer, self).server_bind()
        self.server_address = local_address

        # Multicast group subscription
        mreq = pack("4s4s", socket.inet_aton(self.__mcast_hostport[0]), socket.inet_aton(self.server_address[0]))
        result = \
            self.socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.server_address[0])) or \
            self.socket.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        if result is not None:
            raise socket.error("Problem setting multicast socket options: %d" % socket.errno)
    
    def send ( self, dgram ):
        '''
        Sends the datagram passed as argument to the multicast group
        '''
        return self.socket.sendto(dgram, self.__mcast_hostport)


class McastBridgeHandler(BaseRequestHandler):
    '''
    A handler class for multicast bridges (see class McastBridge).
    Gets the incoming datagram and puts it through to the opposite side of the bridge.
    '''
    def handle ( self ):
        own_addr = self.request[1].getsockname() 
        if self.client_address != own_addr:
            #print("Routing message from %s to %s" % (self.client_address, self.server.peer.grpaddr))
            self.server.peer.send(self.request[0])
    
class McastBridge(McastServer):
    '''
    A multicast server that pushes received data through a peer server
    '''
    def __init__ ( self, hostport, mcast_hostport, ttl=32, peer=None ):
        super(McastBridge, self).__init__(mcast_hostport, hostport, McastBridgeHandler, ttl)
        self.__peer = peer
        
    def getPeer ( self ):
        return self.__peer
    def setPeer ( self, peer ):
        self.__peer = peer
    def removePeer ( self ):
        del self.__peer
    peer = property(getPeer, setPeer, removePeer)
    
class McastRouter(object):
    '''
    A router joining two multicast domains by means of two bridges peered to each other
    '''
    def __init__ ( self, hostport1, mcast_hostport1, hostport2, mcast_hostport2, func = None ):
        '''
        Constructor
        '''
        self.__server1 = McastBridge(hostport1, mcast_hostport1)
        self.__server2 = McastBridge(hostport2, mcast_hostport2, 32, self.__server1)
        self.__server1.peer = self.__server2
        self.__func = func
        self.__shutdown = False

    def route_forever ( self ):
        try:
            while not self.__shutdown:
                r, w, e = select([self.__server1.socket, self.__server2.socket], [], [], 0.5)
                if self.__server1.socket in r:
                    self.__server1.handle_request()
                if self.__server2.socket in r:
                    self.__server2.handle_request()
        finally:
            self.__server1.socket.close()
            self.__server2.socket.close()
            self.__shutdown = False

    def shutdown ( self ):
        self.__shutdown = True

    @property
    def server1 ( self ):
        return self.__server1
    
    @property
    def server2 ( self ):
        return self.__server2


SequencedMessage = namedtuple('SequencedMessage', 'seq, epoch, ack, body')


class SequencedDgramMsgHandler(BaseRequestHandler):
    '''
    A handler for datagram senders of sequenced messages.
    Its only function is decoding the sequenced message and delivering it to its server.
    '''
    def handle ( self ):
        msg = self.decode()                                 # decode msg
        self.server.receive(msg, self.client_address[0])    # hand over to server
    
    def decode ( self ):
        return SequencedMessage(*unpack('QQQ%ds' % (len(self.request[0]) - 3*calcsize('Q')), self.request[0]))


class SequencedMsgSndQueue(deque):
    '''
    A normal deque providing a send-queue-like interface. This interface creates
    a sliding-window abstraction.
    
    The queue has a sequence number property containing the sequence number of the
    last message pushed into the queue. Each time a message is pushed into the
    queue it gets the sequence number queue.seq+1 and the queue's 'seq' property
    is updated accordingly.
    
    The queue has also an 'ack' property containing the sequence number
    in the last positive acknowledge received from any peer. The class keeps a counter
    of the number of times the same acknowledged sequence number has been seen.
    
    The current sending window ranges from the queue's head to the message having
    as sequence number the value of the queue's 'ack' property.
    
    By default the queue grows without bound, but its maximum size can be limited by
    argument 'maxlen' to the class' constructor.
    
    The queue can be trimmed down to the current transmission
    window (thus "forgetting" about all the acknowledged messages) calling its tail()
    method.
    
    Notice that trimming down the queue in a RMcast environment where just one peer
    has sent a positive ack for a message may cause it to forget about messages not
    received by other peers. If a nak for a forgotten message comes in later the
    queue will be unable to re-transmit it. 
    '''
    def __init__ ( self, seq=0, maxlen=None ):
        '''
        Constructor.
        'seq' argument is the sequence number the queue is initialized with. The
        first message pushed into the queue shall get this sequence number plus one.
        By default it takes the value 0.
        'maxlen' is used to control the maximum size of the queue. By default it
        takes the value None, i.e. no maximum size limit.
        '''
        super(SequencedMsgSndQueue, self).__init__(maxlen=maxlen)
        self.__seq = seq
        self.__ack = [0, 0]
    
    @property
    def seq ( self ): return self.__seq

    @seq.setter
    def seq ( self, seq ): self.__seq = seq
    
    @property
    def ack ( self ): return self.__ack[0]

    @property
    def ackcount ( self ): return self.__ack[1]

    @property
    def empty ( self ): return len(self) == 0

    def push ( self, msg ):
        self.__seq += 1
        self.append(msg)

    def updateack ( self, ack ):
        if ack > self.ack:
            self.__ack = [ack, 1]
        else:
            self.__ack[1] += 1

    def tail ( self ):
        if not self.empty:
            # Remove all messages from send queue's tail with seq < ack
            while self[0].seq < self.ack:
                self.popleft()

class PersistentSequencedMsgSndQueue(SequencedMsgSndQueue):
    '''
    WORK IN PROGRESS
    
    A SequencedMsgSndQueue that persists the messages it receives to disk.
    When the class is instantiated, if actual argument 'id' to the constructor
    matches that of a previous instance the queue starts initialized to the
    contents of the previous instance.
    
    The class can handle message storage in files containing chunks of 'seglen'
    messages.
    Files are named as 'id'.<no>, where <no> is the chunk number.
    If 'seglen' actual argument to the constructor is not provided or takes
    the value None then all messages are stored in a single file with name 'id'.
    '''
    def __init__ ( self, id_, maxlen=None, seglen=None ):
        '''
        Constructor
        '''
        super(PersistentSequencedMsgSndQueue, self).__init__(0, maxlen)
        self.__id = id_
        self.__seglen = seglen
        self.__shelf = shelve.open(self.__id)
        self.seq = int(max(self.__shelf or [0]))

    @property
    def id ( self ): return self.__id

    @property
    def seglen ( self ): return self.__seglen
    
    def push ( self, msg ):
        self.__shelf[str(msg.seq)] = msg.body
        super(PersistentSequencedMsgSndQueue, self).push(msg)

    def empty ( self ):
        return super(PersistentSequencedMsgSndQueue, self).empty() and len(self.__shelf) == 0
    
    def __cycleshelf ( self ):
        self.__shelf.close()
        files = filter(lambda f: re.match('^' + self.__id + "\.\d+$", f), os.listdir())
        files.sort()
        index = int(re.search("\d+$", files[-1]).group())
        os.rename(self.__id, self.__id + '.' + str(index+1))
        self.__shelf = shelve.open(self.__id)
        
    def __getitem__ ( self, key ):
        if super(PersistentSequencedMsgSndQueue, self).__getitem__(key) is None:
            return self.__shelf[key]

class SequencedMsgRcvQueue(list):
    '''
    A queue for reception of sequenced messages.
    The queue sorts messages by sequence number (msg.seq) using the heap queue algorithm.
    Messages can be appended to the queue's tail using push(), and removed from the
    queue's head using pop().
    The head() method is a generator returning the longest sequence of messages with
    consecutive sequence numbers at the queue's head.
    The class provides support for reliable transmission protocols. The 'nakseen'
    property can be used to record if a NAK for a message currently in the queue has
    been spotted. The startTimer() and cancelTimer() methods can be used to schedule
    queue management events, like e.g. sending of a NAK if a message is missing.
    The class itself is not thread-safe but provides a 'lock' property of type Lock
    to synchronize access to the queue.
    '''
    def __init__ ( self, ack=1, epoch=0 ):
        '''
        Constructor
        '''
        super(SequencedMsgRcvQueue, self).__init__()
        self.__ack = ack
        self.__epoch = epoch
        self.__nakseen = False
        self.__nakt = None
        self.__lock = Lock()

    @property
    def naktime ( self ): return uniform(0.5, 1.0)

    @property
    def empty ( self ): return len(self) == 0
    
    @property
    def ack ( self ): return self.__ack
    
    @property
    def epoch ( self ): return self.__epoch
    
    def getNakSeen ( self ):
        return self.__nakseen
    def setNakSeen ( self, val ):
        self.__nakseen = val
    nakseen = property(getNakSeen, setNakSeen)

    @property
    def lock ( self ): return self.__lock
    
    def push ( self, msg ):
        heappush(self, msg)
        
    def pop ( self ):
        return heappop(self)
    
    def startTimer ( self, func, args ):
        if self.__nakt is None:
            self.__nakt = Timer(self.naktime, func, args=args)
            self.__nakt.start()
        
    def cancelTimer ( self ):
        if self.__nakt:
            self.__nakt.cancel()
            self.__nakt = None
          
    def head ( self ):
        while not self.empty and self[0].seq == self.__ack:
            msg = self.pop()
            self.__ack +=1
            yield msg
            self.__nakseen = False


class RMcastServer(McastServer):
    '''
    A reliable multicast server. It adds message acknowledgement and re-transmission 
    to the basic McastServer. Additionally, if actual argument 'lossless' is set to
    'True' when instantiating a server it provides resilience to process failures
    and re-starts, guaranteeing that messages are eventually delivered even in the
    presence of such events.
    
    The server holds a sending queue, where it stores all the messages it has sent
    just in case some peer asks for a retransmission. Latest messages sent are held
    in RAM, whereas older messages are stored in disk. For the time being messages
    are never deleted from the disk once stored. 
     
    The server also holds one queue for every sender (i.e. source IP address) it sees.
    For every queue, the server holds the sequence number of the last message
    received in a consecutive sequence (i.e., the last message received if no
    losses have taken place, or the last message before the first loss). This
    sequence number is bound to the 'ack' variable within the corresponding queue.
    Every queue's 'ack' variable is reliably stored in disk.
    
    When the server sees a message from a sender which sequence number is higher
    than the corresponding queue's 'ack' value it checks whether it has seen a
    NAK message for that sequence number (NAK messages are multicast by RMcast
    servers). If it has it just sits and waits for the retransmission, otherwise
    it multicast a NAK of its own for the missing sequence number.
    
    Message exchange takes place as follows:
    
    S              R1             R2             R3
    |              |   seq=1      |              |
    |------------->|------------->|------------->|
    |              + (Deliver msg 1 to handler)  |
    |              |              + (Deliver msg 1 to handler)
    |              |              |              + (Deliver msg 1 to handler)
    |              |   seq=2      |              |
    |------------->|-------X      |------X       |
    |              + (Deliver msg 2 to handler)  |
    |              |   seq=3      |              |
    |------------->|------------->|------------->|
    |              + (Deliver msg 3 to handler)  |
    |              |              + (Missing message, start NAK timer)
    |              |              |              + (Missing message, start NAK timer)
    |              |   seq=4      |              |
    |------------->|------------->|------------->|
    |              + (Deliver msg 4 to handler)  |
    |              |   seq=5      |              |      3 messages after missing message = message loss
    |------------->|------------->|------------->|     -------------------------------------------------  
    |              + (Deliver msg 5 to handler)  |                      |               |
    |              |              + (Re-start NAK timer and send NAK) <-+               |
    |              |   ack=2      |              |                                      |
    |<-------------|<-------------|------------->|                                      |
    |              |   ack=2      |              + (Re-start NAK timer and send NAK)  <-+
    |<-------------|<-------------|<-------------|
    |              |              + (Saw NAK from R3, cancel NAK timer)
    |              |   seq=2      |              |
    |------------->|------------->|------------->|
    |              + (Duplicated, discard)       |
    |              |              + (Deliver msgs 2,3,4,5 to handler)
    |              |              |              + (Deliver msgs 2,3,4,5 to handler)
    |              |   seq=6      |              |
    |------------->|------------->|------------->|
    |              |              |              |

    In special circumstances like e.g. when a peer is isolated from the network
    it might happen that peer's NAKs get nowhere. Each RMcastServer instance can
    be initialized to give up NAK'ing after a number of retries. When an instance
    gives up it raises an exception the upper layer (the 'handler' of the RMcastServer
    instance) must catch and react to accordingly. See the following diagram.
    
    S              R1             R2             R3
    |              |   seq=1      |              |
    |------------->|------------->|------------->|
    |              + (Deliver msg 1 to handler)  |
    |              |              + (Deliver msg 1 to handler)
    |              |              |              + (Deliver msg 1 to handler)
    |              |   seq=2      |              |
    |------------->|-------X      |------X       |
    |              + (Deliver msg 2 to handler)  |
    |              |   seq=3      |              |
    |------------->|------------->|------------->|
    |              + (Deliver msg 3 to handler)  |
    |              |              + (Missing message, start NAK timer)
    |              |              |              + (Missing message, start NAK timer)
    |              |              |              |
    |              |              |       -------+------ (R3 is isolated from network)
    |              |              |              |
    |              |   ack=2      + (R2 times out and sends NAK)
    |<-------------|<-------------|-------X      |
    |              |   seq=2      |              |
    |------------->|------------->|-------X      |
    |              + (Duplicated, discard)       |
    |              |              + (Deliver msgs 2,3 to handler)
    |              |              |              |
    |              |              |              + (R3 times out and sends NAK)
    |              |              |       X------|
    |              |              |              |
    |              |              |              + (R3 times out and sends NAK)
    |              |              |       X------|
    |              |              |              |
    |              |              |              + (R3 times out, gives up and notifies upper layer)
    |              |              |              |
    
    When there are multiple senders, each one piggy-backs the ack corresponding to
    the last message received on messages it sends; this serves as a signal to the
    group that the sender got that message, though the current implementation does
    nothing with this 'ack' value.
    
    Multi-sender message exchange takes place as follows:
    
    S1             S2             S3             S4
    |              |   seq=x      |              |
    |------------->|------------->|------------->|
    |              + (Deliver msg x to handler)  |
    |              |              + (Deliver msg x to handler)
    |              |              |              + (Deliver msg x to handler)
    |              |  seq=y,ack=x |              |
    |<-------------|------------->|------------->|
    + (Deliver msg y to handler)  |              |
    |              |              + (Deliver msg y to handler)
    |              |              |              + (Deliver msg y to handler)
    |              |  seq=z,ack=y |              |
    |<-------------|<-------------|------------->|
    + (Deliver msg z to handler)  |              |
    |              + (Deliver msg z to handler)  |
    |              |              |              + (Deliver msg z to handler)
    
    The following only applies when actual argument 'lossless' is not present
    or is present but set to 'False' when instantiating the server.
    
    Another special circumstance takes place when a sender fails. In this case
    NAKs addressed to the failed sender shall not be answered and those peers
    missing the NAK'd message shall abort. The upper layer is responsible for
    dealing with this situation. Notice that if a sender fails then restarts,
    its first message shall carry sequence number 1 so receivers having
    received something from this sender before would discard the first and a few
    forthcoming messages as duplicates. To prevent this from happening, messages
    carry an 'epoch' field that is monotonically increased every time a sender
    starts. When a receiver sees a message carrying an 'epoch' field higher than
    the last 'epoch' field seen from its sender, it acts as if it were the first
    time it sees a message from this sender.
    
    Finally, when a receiver fails then starts again it would keep NAK'ing with
    ack=1 (it's missing everything from the first message) then aborting after
    a while. To prevent this from happening, RMcastServer instances 'sync' to
    their peers' sequence numbers on the first message they receive from each
    peer.
     
    The implementation is unable to keep the group in sync under the following
    circumstances:
    
    - a message is dropped by the network and its sender restarts: unless the message
      has been dropped before it reaches any peer, those peers missing it shall never
      receive a re-transmission (the sender 'forgot' its send queue on restart) and
      give-up asking for it, notifying upper layer
      
    - a receiver restarts: when coming back the receiver doesn't know how many messages
      it's missed during its absence (receivers 'forget' the 'ack' values for the
      senders they know on restart)
      
    - the network is down for longer than afforded by the 'max_nak_retries' value of any
      instance affected by the outage (affected instances shall give-up asking for a
      retransmission and notify upper layer)
      
    When the actual argument 'lossless' is set to 'True' when instantiating the server,
    aLL messages are eventually delivered in all cases as long as receivers missing a
    message keep NAK'ing. Hence the assumption is a failed sender shall restart after
    a while, otherwise a receiver missing a message shall either abort once the max
    number of NAKs has been reached, or block holding a number of messages in its receive
    queue for the dead sender forever.
    '''
    
    #Constants
    SND_EXCEPTION = 0
    RCV_EXCEPTION = 1
    
    @staticmethod
    def ntoi ( addr ):
        ''' Converts a packed 32-bits IPv4 address into an integer number '''
        return reduce(add, map(lambda a,b: a*(1 << 8*b), addr, range(3, -1, -1)))

    @staticmethod
    def iton ( baddr ):
        ''' Converts an integer number into a packed 32-bits IPv4 address '''
        return bytes([(baddr >> 8*n) & 0xFF for n in range(4)])
    
    # Properties    
    id = property(lambda s: s.__id)
    epoch = property(lambda s: s.__epoch)
    ack = property(lambda s: s.__ack)
    nakretries = property(lambda s: s.__nakretries)
    lossless = property(lambda s: s.__shelf is not None)
    
    def __init__ ( self, mcast_hostport, hostport, handler, ttl=32, station_id=None, max_nak_retries=3, lossless=False, filesize=10000 ):
        '''
        Constructor.
        '''
        super(RMcastServer, self).__init__(mcast_hostport, hostport, SequencedDgramMsgHandler, ttl)
        self.__id = station_id or (str(mcast_hostport) + '@' + str(hostport))
        self.__rcvq = {}
        self.__ack = 0
        self.__handler = handler
        self.__nakretries = max_nak_retries
        self.__epoch = int(clock())
        self.__shelf = None
        lastseq = 0
        if lossless:
            # Loss-less servers don't use epochs, they don't need to detect sender crashes 
            self.__epoch = 0
            
            self.__shelf = shelve.open(self.__id)
            
            self.__rcvshelf = self.__shelf
            if self.__rcvshelf is not False:
                for (from_addr, ack) in self.__rcvshelf.items():
                    self.__rcvq[from_addr] = SequencedMsgRcvQueue(ack, self.__epoch)
                    
            lastseq = int(max(self.__shelf or [0]))
                                
        self.__sndq = SequencedMsgSndQueue(lastseq)                
            
        # We never know if the last message recorded was actually sent - the process might have
        # crashed after recording it but right before sending it. Hence, just in case we send it
        # in all cases. In the worst case we're causing just one unnecessary retransmission.
        if lastseq > 0:
            self.resend(lastseq)
            
    def rcvq ( self, from_addr, msg=None ):
        '''
        Returns the receive queue that corresponds to the sender identified by its address,
        'from_addr'.
        If the queue doesn't exist (because we've never seen a message from that sender)
        and actual argument 'msg' is present, a new one is created using 'msg.seq' and
        'msg.epoch' as initial values.
        If the queue doesn't exist (because we've never seen a message from that sender)
        and actual argument 'msg' is absent or set to 'None', exception KeyError is risen.
        '''
        try:
            rcvq = self.__rcvq[from_addr]
        except KeyError as e:
            if msg is None: raise e
            rcvq = SequencedMsgRcvQueue(msg.seq, msg.epoch)
            self.__rcvq[from_addr] = rcvq
        return rcvq
    
    def receive ( self, msg, from_addr ):
        #print("*** Received from %s (seq=%d ack=%d): %s" % (from_addr, msg.seq, msg.ack, msg.body))
        
        # Check for OOB message
        if msg.seq == 0:
            r = self.__handler.handleOOB(msg.body, from_addr)
            if r is not None:
                super(RMcastServer, self).send(r, from_addr)
            return
        
        baddr, ack = msg.ack & 0xFFFFFFFF, msg.ack >> 32
        self_baddr = RMcastServer.ntoi(socket.inet_aton(self.server_address[0]))
        from_baddr = RMcastServer.ntoi(socket.inet_aton(from_addr))
        
        if len(msg.body) > 0:
            rcvq = self.rcvq(from_addr, msg)        
            rcvq.lock.acquire()
            try:            
                # Handle the incoming message; first, check if sender has restarted
                # (notice when servers are loss-less epochs are always 0 so the code
                # below should never be entered in that case) 
                if msg.epoch > rcvq.epoch:
                    # The sender is loss-full and has re-started; drop whatever
                    # we may have in the receive queue and notify the upper layer
                    if not rcvq.empty:
                        self.__handler.handleException(RMcastServer.RCV_EXCEPTION, baddr)
                    del self.__rcvq[from_addr]
                    if self.lossless:
                        del self.__rcvshelf[from_addr]
                    return self.receive(msg, from_addr)
                
                if msg.seq < rcvq.ack: return # duplicated message

                rcvq.push(msg)
                for m in rcvq.head():
                    # Handle the top message in the receive queue
                    r = self.__handler.handle(m.body, from_addr)
                    
                    # We need to update the last message handled with every handling,
                    # since in case of failure in the middle of this loop we need to
                    # know which was the last one processed
                    if self.lossless:
                        self.__rcvshelf[from_addr] = rcvq.ack
                    self.__ack = (rcvq.ack << 32) + from_baddr
                        
                    # If there was an answer send it back
                    if r is not None:
                        self.send(r)
            finally:
                rcvq.lock.release()
                
            self.checkmissing(rcvq, baddr)
            
            # Handle the ack only if it refers to a message sent by us
            if baddr == self_baddr:
                self.__sndq.updateack(ack)
                if self.__sndq.ackcount >= 3:
                    self.resend(ack)        # re-transmit if we've seen 3 times the same old ack
                if self.lossless:
                    self.__sndq.tail()      # purge send queue only if loss-less (we've got the shelf back-up)
        else:
            # Handle the NAK
            if baddr == self_baddr:         # if it is for a message sent by us ...
                self.resend(ack)            # ... re-transmit immediately
            else:                           # otherwise ...
                self.spottednak(baddr, ack) # ... record if we saw a NAK for someone else
            
    def checkmissing ( self, rcvq, baddr, tries=0 ):
        rcvq.lock.acquire()
        try:
            if not rcvq.empty:
                # Not all messages could be delivered, see how bad it is
                if len(rcvq) >= 3:
                    # If 3+ messages queued cancel timer (this combined with the call to startTimer()
                    # below has the effect of resetting the timer) and send NAK for the first missing message
                    rcvq.cancelTimer()
                    if not rcvq.nakseen:
                        self.sendnak(baddr, rcvq.ack)
                
                if tries < self.__nakretries:
                    # Schedule a time-out, just in case we don't receive from this peer in a while
                    rcvq.startTimer(RMcastServer.checkmissing, (self, rcvq, baddr, tries+1))
                else:
                    # max time-outs in a row so we give up - notify upper layer
                    self.__handler.handleException(RMcastServer.RCV_EXCEPTION, baddr)
        finally:
            rcvq.lock.release()
        
    def spottednak ( self, baddr, ack ):
        addr = socket.inet_ntoa(RMcastServer.iton(baddr))
        try:
            rcvq = self.__rcvq[addr]
            if ack >= rcvq.ack and ack < rcvq[0].seq:
                # NAK is for a message we're missing so we can stop asking (the other guy will do it)
                rcvq.cancelTimer()
                rcvq.nakseen = True
        except KeyError:
            # We've seen a NAK for a message from someone we don't know; since we'll see the retransmission
            # too we have nothing to worry about
            pass
        except IndexError:
            # The receive queue for that sender is empty so we're not missing any message from it
            pass
        
    def send ( self, data, dst = None ):
        if (dst is None) or (dst == self.grpaddr):
            msg = SequencedMessage(seq = self.__sndq.seq+1, epoch = self.__epoch, ack = self.__ack, body=data)
            if self.lossless:
                self.__shelf[str(msg.seq)] = msg.body
            self.__sndq.push(msg)
        else:
            msg = SequencedMessage(seq = 0, epoch = self.__epoch, ack = self.__ack, body=data)
        return super(RMcastServer, self).send(self.encode(msg))
        
    def sendnak ( self, baddr, ack ):
        nak = (ack << 32) + baddr
        msg = SequencedMessage(seq = self.__sndq.seq or 1, epoch = self.__epoch, ack = nak, body=None)
        super(RMcastServer, self).send(self.encode(msg))
        
    def resend ( self, seq ):
        try:
            if seq < self.__sndq[0].seq:
                raise IndexError("Message with sequence number %d not in memory" % seq)
            msg = self.__sndq[seq - self.__sndq[0].seq]
        except IndexError:
            try:
                if not self.lossless:
                    raise KeyError("Message with sequence number %d not in persistent store" % seq)
                data = self.__shelf[str(seq)]
                msg = SequencedMessage(seq = seq, epoch = self.__epoch, ack = self.__ack, body=data)
            except KeyError:
                return self.__handler.handleException(RMcastServer.SND_EXCEPTION, seq)
                
        return super(RMcastServer, self).send(self.encode(msg))
                
    def encode ( self, msg ):
        return pack('QQQ%ds' % len(msg.body or b''), msg.seq, msg.epoch, msg.ack, msg.body or b'')

    def cycleshelf ( self ):
        if not self.lossless: return
        self.__shelf.close()
        files = filter(lambda f: re.match('^' + self.__id + "\.\d+$", f), os.listdir())
        files.sort()
        index = int(re.search("\d+$", files[-1]).group())
        os.rename(self.__id, self.__id + '.' + str(index+1))
        self.__shelf = shelve.open(self.__id)
        
    def shutdown ( self ):
        result = super(RMcastServer, self).shutdown()
        if self.lossless:
            self.__shelf.close()
            self.__rcvshelf.close()
        list(map(SequencedMsgRcvQueue.cancelTimer, self.__rcvq.values()))
        return result


class ProtocolAgent(type):
    '''
    A meta-class for classes intended to talk over some communication means.
    Currently supports communication over:
        - local memory (classes running within the same Python interpreter)
        - TCP
        - UDP
        - Reliable multi-cast (using the RMcastServer implementation in this module)
    '''
    def __new__ ( cls, name, bases, d ):
        '''
        Builds the agent class
        '''
        ProtocolAgent.__addmsghandlers(bases, d)
        ProtocolAgent.__decoratesend(bases, d)
        ProtocolAgent.__addgenericjsonhandler(d)
        ProtocolAgent.__exportprivate(bases, d)
        return type.__new__(cls, name, bases, d)

    @staticmethod
    def __searchbases ( bases, name ):
        '''
        Searches the base classes' dictionaries for the value
        bound to the first key matching the input argument 'name'.
        '''
        dicts = map(lambda b: b.__dict__, bases)
        m = map(lambda d: d.get(name), filter(lambda d: name in d, dicts))
        try:
            return next(m)
        except StopIteration:
            return None

    @staticmethod
    def __addmsghandlers ( bases, d ):
        '''
        Adds to the class' dictionary any method having a 'msgname' attribute in a base class;
        those methods can be tagged in base classes using the 'handles(msg)' decorator.
        Each method is bound to a name of the form <msg>Handler and is called by the generic JSON
        handler mentioned in class method __addgenericjsonhandler(). 
        '''
        handlers = [
            (m.msgname+'Handler', m)
            for b in bases for m in b.__dict__.values()
            if hasattr(m, 'msgname') and callable(m)
        ]
        d.update(handlers)
    
    @staticmethod
    def __decoratesend ( bases, d ):
        '''
        Decorates the first 'send' method found in base classes by wrapping it into
        the jsonencoded() method.
        '''
        send = d.get('send', ProtocolAgent.__searchbases(bases, 'send'))
        if send is not None and callable(send):
            d['send'] = ProtocolAgent.jsonencoded(send)
        else:
            d['send'] = lambda s, m, d: None
            #d['send'] = lambda s, m, d: \
            #    print("Automatically-generated no-op %s.send() method: msg=%s, dst=%s" % \
            #    (type(s).__name__, str(m), str(d)))
    
    @staticmethod
    def __addgenericjsonhandler ( d ):
        '''
        Adds a handler method that calls the right message handler method based on the
        type of the message received.
        '''
        def defaulthandler ( self, message, src ):
            '''
            Called when there is no handler defined for a message.
            Can be re-implemented in derived classes.
            '''
            msgname, msgval = message.decode().split(':', 1)
            print("%s.defaulthandler: no handler found for message '%s' received from %s" % (type(self).__name__, msgname, src))
        
        def unknownhandler ( self, message, src ):
            '''
            Called when we receive an unknown message.
            Can be re-implemented in derived classes.
            '''
            msgname, msgval = message.decode().split(':', 1)
            print("%s.unknownhandler: unknown message '%s' received from %s" % (type(self).__name__, msgname, src))
        
        def handle ( self, message, src ):
            '''
            Method to be injected into classes having ProtocolAgent as metaclass.
            Decodes a received JSON message into a Python object and calls the handler
            method 'self.<msgname>Handler()', where <msgname> is the message name received
            at the heading of the message.
            '''
            msgname, msgval = message.decode().split(':', 1)
            if msgname in dir(type(self)):
                MsgType = type(self).__bases__[0].__dict__[msgname]
                msg = MsgType(**json.loads(msgval))
                try:   
                    return type(self).__dict__[msgname + 'Handler'](self, msg, src)
                except KeyError:
                    return self.defaulthandler(message, src)
            else:
                return self.unknownhandler(message, src)
            
        d['handle'] = handle
        d['unknownhandler'] = unknownhandler
        d['defaulthandler'] = defaulthandler
        
    @staticmethod
    def __exportprivate ( bases, d ):
        '''
        Surfaces any method in a base class tagged with the 'export' attribute.
        Methods in base classes can be tagged using the 'export' decorator.
        '''
        exported = [
            (m.__name__, m)
            for b in bases for m in b.__dict__.values()
            if hasattr(m, 'export')
        ]
        d.update(exported)
        
    @staticmethod
    def export ( privatefunc ):
        '''
        Decorator method, adds an 'export=True' attribute to privatefunc
        '''
        privatefunc.export = True
        return privatefunc
    
    @staticmethod
    def handles ( msgname ):
        '''
        Decorator factory method, produces message handler decorators.
        Returns a decorator for the method that shall handle the message with name 'msgname'.
        '''
        def decorator ( handlerfunc ):
            @wraps(handlerfunc)
            def wrapper ( self, msg, src ):
                rsp = handlerfunc(self, msg, src)
                if type(rsp).__name__.endswith('Msg'):  # type(None).__name__ is 'NoneType'
                    jsonencodedrsp = type(rsp).__name__ + ':' + json.dumps(rsp.__dict__)
                    return bytes(jsonencodedrsp, 'utf8') 
            wrapper.msgname = msgname
            return wrapper
        return decorator
    
    @staticmethod
    def jsonencoded ( sendfunc ):
        '''
        Decorator for a method having as arguments a message 'msg' and a destination 'dst'.
        It encodes the message using JSON encoding before calling the decorated method.
        Ideally suited to decorate a send() method receiving a Python object as message.
        '''
        @wraps(sendfunc)
        def wrapper ( self, msg, dst=None ):
            jsonencodedmsg = type(msg).__name__ + ':' + json.dumps(msg.__dict__)
            return sendfunc(self, bytes(jsonencodedmsg, 'utf8'), dst)
        return wrapper

    def describe ( self ):
        '''
        Returns a string description of the agent class with messages, message handling methods,
        and surfaced methods
        '''
        messages = \
            map(lambda atr: "\t" + atr + "\n",
                filter(lambda m: m.endswith("Msg"), self.__dict__.keys()))
        handlers = \
            map(lambda mth: "\t" + mth.__name__ + " handles " + mth.msgname + "\n",
                filter(lambda m: hasattr(m, 'msgname'), self.__dict__.values()))
        surfaced = \
            map(lambda mth: "\t" + mth.__name__ + "\n",
                filter(lambda m: hasattr(m, 'export'), self.__dict__.values()))
        return "Name: %s\nMessages:\n%s\nHandlers:\n%s\nSurfaced:\n%s" % (self.__name__, messages, handlers, surfaced)
    
    @staticmethod
    def local ( cls ):
        '''
        A class decorator to support communication between agents within the same Python interpreter.
        Usage:
            @ProtocolAgent.local
            class A:
                ...
        '''
        class LocalLink ( object ):
            def send ( self, msg, dst ):
                dst.handle(msg, self)
                
        class wrapper(cls, LocalLink, metaclass=ProtocolAgent ):
            def __init__ ( self, *args, **kwargs ):
                cls.__init__(self, *args, **kwargs)

            def address ( self ):
                return self
            
        return wrapper
                
    @staticmethod
    def UDP ( cls ):
        '''
        A class decorator to support communication between agents through UDP.
        Usage:
            @ProtocolAgent.UDP
            class B:
                ...
        '''
        class UDPHandler(BaseRequestHandler):
            def handle ( self ):
                data = self.request[0].strip()
                socket = self.request[1]
                result = self.server.handle(data, self.client_address)
                if result is not None:
                    socket.sendto(result, self.client_address)
                
        class wrapper(cls, UDPServer, metaclass=ProtocolAgent):
            def __init__ ( self, hostport, *args, **kwargs ):
                UDPServer.__init__(self, hostport, UDPHandler)
                cls.__init__(self, *args, **kwargs)
                
            def address ( self ):
                return self.server_address
            
            def send ( self, msg, dst ):
                # The method providing send() functionality in UDPServer is sendto()
                return self.socket.sendto(msg, dst) < len(msg)

        return wrapper
    
    @staticmethod
    def TCP ( cls ):
        '''
        A class decorator to support communication between agents using TCP.
        Usage:
            @ProtocolAgent.TCP
            class C:
                ...
        '''
        class TCPHandler(BaseRequestHandler):
            def handle ( self ):
                sock = self.request
                peer = sock.getpeername()
                if peer not in self.server.peers: self.server.addpeer(peer, sock)
                try:
                    while True:
                        data = sock.recv(2)
                        if len(data) == 0: break    # remote peer closed the connection
                        msglen = (data[0] << 8) + data[1]
                        data = sock.recv(msglen)
                        result = self.server.handle(data, self.client_address)
                        if result is not None:
                            sock.send(result)
                    self.server.delpeer(peer)
                    self.server.closed(peer)
                except socket.error as msg:
                    logger.warning("Error sending result message to remote peer %s, cause: %s" % (self.client_address, msg))
                
        class ThreadingTCPServer(ThreadingMixIn, TCPServer): pass
        
        class wrapper(cls, ThreadingTCPServer, metaclass=ProtocolAgent):
            def __init__ ( self, hostport, *args, **kwargs ):
                TCPServer.__init__(self, hostport, TCPHandler)
                cls.__init__(self, *args, **kwargs)
                self.__peers = {}
                self.__peersmutex = Lock()
            
            @property
            def peers ( self ): return self.__peers
            
            def addpeer ( self, address, sock ):
                with self.__peersmutex:
                    self.__peers[address] = sock
                    
            def delpeer ( self, address ):
                with self.__peersmutex:
                    del self.__peers[address]
                    
            def address ( self ):
                return self.server_address
            
            def send ( self, msg, dst ):
                # Notice TCPServer is a pure reactive server class, it doesn't have a send() method
                # Hence this wrapper needs to provide its own send()
                # There is a potential race condition here, if the try block runs in parallel to the
                # closing of a connection by the remote peer. The try block gets the socket, the
                # connection close removes the socket, then we try to send on a closed socket. The
                # consequence is the send() call below raises an exception.
                try:
                    sock = self.__peers[dst]
                except KeyError:
                    with self.__peersmutex:
                        # Two threads executing this block in sequence shall create two
                        # connections to the same dest. Hence before actually creating the
                        # connection we need to check if another thread has created it already.
                        if not dst in self.__peers:
                            # create a connection to dst
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            #sock.bind((self.address[0], self.address[1]+len(self.__peers)+1))
                            sock.connect(dst)
                            self.__peers[dst] = sock
                            
                # Notice socket.send() returns the actual number of bytes sent, which may be smaller
                # than the number of bytes we want to send; we're responsible for sending the remaining
                # bytes. With current TCP window sizes however it's highly unlikely that fragmented TCP
                # segment delivery takes place so we try to send everything first, and only if not all
                # was sent we enter the delivery loop that copies the message buffer due to slicing.
                enc_msg = bytes((len(msg) >> 8, len(msg) & 0xFF)) + msg
                bytes_remaining = len(enc_msg) - sock.send(enc_msg)
                while bytes_remaining:
                    bytes_remaining = bytes_remaining - sock.send(enc_msg[len(enc_msg)-bytes_remaining:])
            
            def close ( self ):
                # We need to protect ourselves from the handler class removing
                # a peer while we're iterating over the list
                with self.__peersmutex:
                    for addr in self.__peers: self.__peers[addr].close()
                self.socket.close()
            
        return wrapper
    
    @staticmethod
    def RMcast ( cls ):
        '''
        A class decorator to support communication between agents using reliable multicast.
        Usage:
            @ProtocolAgent.RMcast
            class D:
                ...
        '''
        class wrapper(cls, RMcastServer, metaclass=ProtocolAgent):
            def __init__ ( self, mcast_hostport, hostport, ttl=32, station_id=cls.__name__, *args, **kwargs ):
                # Notice the handler argument to RMcastServer constructor is not a handler in the
                # serversocket sense; it is the class we want to receive the messages handled by the
                # RMcastServer instance, which has its own socketserver-like handler of type SequencedDgramMsgHandler.
                # Since the wrapper class shall have a handle() method injected by the metaclass, this works.
                RMcastServer.__init__(self, mcast_hostport, hostport, self, ttl, station_id)
                cls.__init__(self, *args, **kwargs)

            def address ( self ):
                return self.grpaddr
                
        return wrapper


class RepeatableTimer ( _Timer ):
    FOREVER = -1
    
    def __init__ ( self, interval, function, args=(), kwargs={}, count=-1):
        super(RepeatableTimer, self).__init__(interval, function, args, kwargs)
        self.__count = count
        
    def run ( self ):
        while self.__count != 0:
            super(RepeatableTimer, self).run()
            self.finished.clear() # Timer.run() calls finished.set() right before returning
            self.__count -= 1
        self.finished.set()
            
    def cancel ( self ):
        self.__count = 1
        super(RepeatableTimer, self).cancel()


@ProtocolAgent.TCP
class StateXferAgent(object):
    '''
    A state transfer agent based on TCP. It can both send and receive
    a dictionary-like state (a number of key-value pairs) over TCP to
    a remote peer.
    '''
    ItemMsg = namedtuple('ItemMsg', 'key, value')
    
    def __init__ ( self, state ):
        self.__state = state
        
    @property
    def state ( self ): return self.__state
    
    def xferItem ( self, key, dst ):
        self.send(StateXferAgent.ItemMsg(key, self.state[key]), dst)
        
    def xferState ( self, dst ):
        try:
            for key, value in self.state.items():
                self.send(StateXferAgent.ItemMsg(key, value), dst)
        finally:
            self.shutdown()
    
    def xferStateAsync ( self, dst, callMeWhenDone = None, callMeOnError = None ):
        def func():
            try:
                self.xferState(dst)
                if callMeWhenDone is not None: callMeWhenDone(dst)
            except Exception as e:
                if callMeOnError is not None: callMeOnError(dst, e)
                
        stateXferThread = Thread(target=func, name=type(self).__name__)
        stateXferThread.run()
        return stateXferThread  # Just in case caller needs to join() on it
        
    @ProtocolAgent.handles('ItemMsg')
    def receiveItem ( self, msg, src ):
        self.state[msg.key] = msg.value

    def closed ( self, peer ):
        self.shutdown()


@ProtocolAgent.RMcast    
class LogicalClockServer(object):
    '''
    A server achieving the Agreement and Order requirements from the State Machine approach
    to distributed systems.
    
    Agreement is achieved thanks to the group communication abilities acquired via the
    RMcast decorator.
    
    Order is achieved by means of Lamport's Logical Clock solution to total message ordering.
    Notice the Logical Clock solution may cause wrong (in the user's perspective) orderings
    if an event at a server (e.g. sending a message) may cause an event at another server.
    If this is your case you need a RealClockServer. See the Lamport paper for more details.
    '''
    
    # Constants
    ALIVE = 1
    TROUBLED = 2
    DEAD = 0
    STATE_PORT = 2500
    
    # Protocol
    HelloMsg = namedtuple('HelloMsg', 'id, time')
    ByeMsg = namedtuple('ByeMsg', 'id, time')
    CommandMsg = namedtuple('CommandMsg', 'command, time')
    HeartbeatMsg = namedtuple('HeartbeatMsg', 'time')
    
    def __init__ ( self, state_hostport, state = {}, clk_start=0, hb_time=1, startup_time=3, death_time=30 ):
        '''
        Constructor
        Basically variable initialization, and launching the HB thread.
        @param state_hostport: 2-elements tuple containing an IP address and TCP port which the state server shall bind to
        @param state: dict-like object containing this server's state, defaults to {}
        @param clk_start: initial value of the logical clock for this server, defaults to 0
        @param hb_time: time interval, in seconds, between two successive heart-beat command generation, defaults to 1s
        @param startup_time: number of heart-beat commands this server waits until assuming it is alone, defaults to 3
        @param death_time: number of seconds until a silent peer is regarded dead, defaults to 30s
        '''
        clock() # On Windows, make sure processor time is > hb_time when __hbthread kicks in
        self.__state_hostport = state_hostport
        self.__state = state
        self.__mutex = Lock()
        self.__reqissued = False
        self.__cmdseq = []
        self.__members = {}
        self.__clk = clk_start
        self.__deathtime = death_time
        self.__hbthread = RepeatableTimer(hb_time, LogicalClockServer.heartbeat, args=(self,))
        self.__hbthread.start()
        while True:
            self.sayhello()
            self.__startingup = startup_time
            try:
                self.__acceptstate()    # Either the heartbeat() method or a peer shall take us out of here
                break;
            except Exception as e:
                print("Error while receiving state from peer, trying again: %s" % str(e))
    
    @property
    def mutex ( self ): return self.__mutex
    
    @property
    def clk ( self ): return self.__clk
    
    @property
    def cmdseq ( self ): return self.__cmdseq
    
    @property
    def members ( self ): return self.__members
    
    @property
    def alivemembers ( self ): return filter(lambda t: t[1][2] != LogicalClockServer.DEAD, self.__members.items())
    
    @property
    def deadmembers ( self ): return filter(lambda t: t[1][2] == LogicalClockServer.DEAD, self.__members.items())    
    
    @property
    def state ( self ): return self.__state
    
    @state.setter
    def state ( self, new_state ): self.__state = new_state
    
    @property
    def hbthread ( self ): return self.__hbthread

    @property
    def startingup ( self ): return self.__startingup > 0
    
    def __xferstate ( self, dst ):
        agent = StateXferAgent(self.__state_hostport, self.__state)
        agent.xferStateAsync(dst)

    def __acceptstate ( self ):
        # We need to keep the agent variable in the instance, so heartbeat() can call agent.shutdown()
        self.__agent = StateXferAgent(self.__state_hostport, self.__state)
        self.__agent.serve_forever()
        del self.__agent
        
    def updclknsend ( self, msg, dst=None ):
        '''
        Send a message, increasing own time by 1. Since own time may be updated from
        here (typically invoked by the application's main thread), from any handle()
        method (typically invoked by the mcast receiving thread), and from the heart-
        beat thread, we need to protect the own time variable using a mutex.
        @param msg: the message to be sent
        @param dst: destination address where the message shall be sent, defaults to None  
        '''
        self.__mutex.acquire()
        try:
            self.__clk = self.__clk + 1
        finally:
            self.__mutex.release()
        self.__reqissued = True
        result = self.send(msg, dst)

        return result
        
    def execute ( self, cmd, dst = None ):
        '''
        Pass a command to the ensemble for execution
        '''
        #print("LogicalClockServer %s: executing command %s at local time %i" % (self.id, cmd, self.__clk))
        return self.updclknsend(LogicalClockServer.CommandMsg(cmd, self.__clk))
    
    def updatepeerstatus ( self ):
        '''
        Goes over the list of alive peers (self.alivemembers) checking the wall clock
        time of the last message we received from each. If it's been one heart-beat time
        (self.hbthread.interval) since the last message tag the peer as troubled
        (LogicalClockServer.TROUBLED), but if it's been more than the death detection time
        (self.deathtime) tag the peer as dead (LogicalClockServer.DEAD).
        '''
        #print("LogicalClockServer.heartbeat(): current time is %f" % clock())
        xpctd_time_of_last_msg = clock() - self.__hbthread.interval
        min_time_of_last_msg = clock() - self.__deathtime
        for src, member in self.alivemembers:
            time_of_last_msg = member[0]
            if time_of_last_msg <= xpctd_time_of_last_msg:
                self.__mutex.acquire()
                try:
                    if time_of_last_msg <= min_time_of_last_msg:
                        self.__members[src] = (time_of_last_msg, member[1], LogicalClockServer.DEAD)
                        #print("LogicalClockServer.heartbeat(): member %s is dead" % src)
                    else:
                        self.__members[src] = (time_of_last_msg, member[1], LogicalClockServer.TROUBLED)
                finally:
                    self.__mutex.release()
        
    def checkstartup ( self ):
        '''
        Manages start-up phase. Decreases time left for phase end and if time's out
        (self.startingup == False) finishes the state transfer agent which the
        constructor is waiting on so it can continue executing.
        '''
        if self.startingup:
            self.__startingup -= 1
            logger.debug(\
                "LogicalClockServer at %s, waiting %d heart-beats for state transfer...",
                self.id, self.__startingup)
            if not self.startingup:
                # Here we might meet any of the following situations:
                # 1) We're alone so no-one has transferred/is transferring state to us
                # 2) Someone has started transferring state to us but transfer is not done yet
                # 3) Someone transferred state to us and transfer is complete
                if len(self.__agent.peers) == 0:
                    logger.debug("LogicalClockServer at %s, giving up on state transfer", self.id)
                    self.__agent.shutdown()
        
    def updclknstatus ( self, msg, src ):
        '''
        Update own time on message reception according to Lamport's logical clocks algorithm.
        Also update status of the message sender. Since both variables are accessed from
        other threads, the whole method is protected with a mutex.
        '''
        self.__mutex.acquire()
        try:
            self.__clk = max(self.__clk + 1, msg.time)
            self.__members[src] = (clock(), msg.time, LogicalClockServer.ALIVE)
        finally:
            self.__mutex.release()
        
    def heartbeat ( self, dst = None ):
        '''
        Pass a fake command to the ensemble in order to enable the stability test.
        Also check the times of the last commands received from known peers and
        if too much time has passed ('too much' is determined by actual argument
        'death_time' passed to the class' constructor, default is 30s) mark the
        silent peer as dead.
        
        This method also manages the start-up phase, putting it to an end when
        the start-up time granted (actual argument 'startup_time' passed to the class'
        constructor, default is 3s) is out.
        
        This method is called from a specialized heart-beat thread. Since the peers'
        stata are accessed from the mcast receiving thread as well (see method
        updclknstatus()), access to peers' stata is protected with a mutex to
        prevent race conditions. 
        '''
        result = self.updclknsend(LogicalClockServer.HeartbeatMsg(self.__clk))
        if self.__reqissued: self.__reqissued = False
                
        self.updatepeerstatus()

        self.checkstartup()
            
        return result

    def handleException ( self, type_, data ):
        '''
        Handle errors reported by the RMcast layer.
        A send error means some peer has asked us to re-send a missing message that we have forgotten.
        A receive error means we've asked for a missing message that was never re-sent.
        '''
        if type_ is RMcastServer.SND_EXCEPTION:
            pass
        elif type_ is RMcastServer.RCV_EXCEPTION:
            pass
        else:
            raise Exception("Exception of unknown type %s reported by RMcast layer with data %s" % (str(type_), str(data)))

    @ProtocolAgent.handles('CommandMsg')
    def handleCommand ( self, msg, src ):
        #print("%s %s: received command %s with time %i at local time %i" \
        #      % (type(self).__name__, self.id, msg.command, msg.time, self.__clk))
        self.updclknstatus(msg, src)
        heappush(self.__cmdseq, (msg.time, msg.command))

    @ProtocolAgent.handles('HeartbeatMsg')
    def handleHeartbeat ( self, msg, src ):
        self.updclknstatus(msg, src)
        #print("LogicalClockServer.handleHeartbeat(): from %s at %f" % (src, clock()))

    @ProtocolAgent.handles('HelloMsg')
    def handleHello ( self, msg, src ):
        self.updclknstatus(msg, src)
        if len(self.__members) > 1:
            # State shall be transferred by us only if we're closest network address
            srcs = list(map(lambda m: m[0], self.alivemembers))
            srcs.sort()
            i = srcs.index(src)
            if i == 0:
                i += 1                      # none before, pick the one after
            elif i == len(srcs) - 1:
                i -= 1                      # none after, pick the one before
            else:
                pass                        # pick the closest one
            if srcs[i] == self.address():   # is it me?
                self.__xferState(src)
        
    @ProtocolAgent.handles('ByeMsg')
    def handleBye ( self, msg, src ):
        self.__mutex.acquire()
        try:
            self.__clk = max(self.__clk + 1, msg.time)
            del self.__members[src]
        finally:
            self.__mutex.release()
        #print("LogicalClockServer.handleBye(): from %s at %f" % (src, clock()))

    @ProtocolAgent.export
    def __iter__ ( self ):
        return self
    
    @ProtocolAgent.export
    def __next__ ( self ):
        if self.isFirstMsgStable():
            return heappop(self.cmdseq)[1]
        else:
            raise StopIteration

    def isFirstMsgStable ( self ):
        '''
        This method carries the stability test for a distributed system of logical
        clocks. A message with sequence number seq is deemed stable if we got at
        least one message with sequence number > seq from each and every *alive* peer.
        
        This implies silent peers block the ensemble until they send something again
        or are declared dead by the heartbeat() method.
        ''' 
        # If the sequence is empty, there're no messages - stable or not
        if len(self.cmdseq) == 0: return False
        
        # If we're alone, all messages in the sequence are stable
        if len(self.members) == 0: return True
        
        # Otherwise, check we got more recent messages from all alive members
        firstmsg = self.cmdseq[0]
        msgtime = firstmsg[0]
        self.mutex.acquire()
        try:
            mintime = min(map(lambda t: t[1][1], self.alivemembers))
        finally:
            self.mutex.release()

        return msgtime < mintime
        
def sayhello ( self, dst = None ):
    return self.updclknsend(LogicalClockServer.HelloMsg(self.id, self.clk))
LogicalClockServer.sayhello = sayhello
            
def saygoodbye ( self, dst = None ):
    return self.updclknsend(LogicalClockServer.ByeMsg(self.id, self.clk))
LogicalClockServer.saygoodbye = saygoodbye
            
def shutdown ( self ):
    self.hbthread.cancel()
    self.saygoodbye()
    super(LogicalClockServer, self).shutdown()
LogicalClockServer.shutdown = shutdown


@ProtocolAgent.RMcast    
class RealClockServer(RMcastServer):
    CommandMsg = namedtuple('CommandMsg', 'command, time')
    
    @ProtocolAgent.handles('CommandMsg')
    def handleCommand ( self, msg, src ):
        pass


def main ( grp_addr, port ):
    class DefaultHandler(object):
        def handle ( self, msg, src ):
            print("received from %s: %s" % (src, msg))
            
    host = socket.gethostbyname(socket.gethostname())
    print("Creating RMcastServer on interface %s bound to %s" % (host, grp_addr))
    server = RMcastServer((host, port), (grp_addr, port), 32, DefaultHandler)
    server.serve_forever()
    
if __name__ == "__main__":
    logging.debug("kk") # No console output in Windows if not
    main("224.1.1.1", 2500)
