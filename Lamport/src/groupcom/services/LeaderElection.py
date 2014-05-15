'''
Created on 13/03/2014

@author: ecejjar
'''

from collections import namedtuple
from threading import Timer, Lock, Thread
from groupcom import server

@server.ProtocolAgent.UDP
class StableLeaderElector(object):
    '''
    Stable leader election as described by Aguilera et al.
    Since the algorithm can deal with lossy links we don't need RMcast.
    '''
    StartMsg = namedtuple('StartMsg', 'round')
    OkMsg = namedtuple('OkMsg', 'round')
    
    @property
    def r ( self ):
        return self.__round
    
    @property
    def n ( self ):
        return len(self.__peers)
    
    @property
    def p ( self ):
        try:
            p = self.__peers.index(self.server_address)
        except ValueError:
            p = self.n
            self.__peers.append(self.server_address)
        return p
    
    @property
    def leader ( self ):
        return self.__leader
    
    @property
    def timer0 ( self ):
        'Timer driving task0 from the stable leader election algorithm'
        return self.__task0
    
    @property
    def timer1 ( self ):
        'Timer driving task1 from the stable leader election algorithm'
        return self.__timer
    
    def restartTimer1 ( self ):
        'Restarts timer1'
        self.__timer and self.__timer.cancel()
        self.__timer = Timer(2*self.__timeout, StableLeaderElector.task1, args=(self,))
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
        self.startRound(0)  # startRound() already starts self.__timer
        self.__task0 = server.RepeatableTimer(timeout, StableLeaderElector.task0, args=(self,))
        self.__task0.start() # for safety, do not start task0 before having started the round
        #...
        
    def startRound ( self, s ):
        '''
        Starts round s by sending a StartMsg to process number s % n
        '''
        #print("*** %s: process %d in %d starting round %d" % ('StableLeaderElector', self.p, self.n, s))
        l = s % self.n
        if self.p != l:
            self.send(StableLeaderElector.StartMsg(s), self.__peers[l])
        self.__round = s
        self.__leader = l
        self.__observer and self.__observer.notify(self)
        self.restartTimer1()

    def task0 ( self ):
        '''
        If I'm leader send OK to everyone. This method is called every self.__timeout seconds.
        '''
        if self.p == self.r % self.n:
            #print("*** %s: leader process %d sending OK to %d peers" % ('StableLeaderElector', self.p, self.n))
            rcvrlist = map(lambda rcvr: self.send(StableLeaderElector.OkMsg(self.r), rcvr), self.__peers)
            if any(rcvrlist):
                pass    # couldn't send to those in rcvrlist, log something?
            
    def task1 ( self ):
        '''
        It's been 2*self.__timeout without OKs from current leader - start new round
        '''
        #print("*** %s: process %d timed-out on round %d" % ('StableLeaderElector', self.p, self.r))
        self.startRound(self.r + 1)  # startRound() already restarts self.__timer

    @server.ProtocolAgent.export
    def shutdown ( self ):
        self.timer0.cancel()
        self.timer1.cancel()
        super().shutdown()
        
    @server.ProtocolAgent.handles('StartMsg')        
    def handleStartMessage ( self, msg, src ):
        #print("*** %s: process %d received Start message for round %d from peer at %s" \
        #      % ('StableLeaderElector', self.p, msg.round, src))
        if src not in self.__peers: self.__peers.add(src)
        k = msg.round
        if k > self.r:
            self.startRound(k)
    
    @server.ProtocolAgent.handles('OkMsg')        
    def handleOkMessage ( self, msg, src ):
        #print("*** %s: process %d received Ok message for round %d from peer at %s" \
        #      % ('StableLeaderElector', self.p, msg.round, src))
        if src in self.__peers:
            k = msg.round
            if k == self.r:
                self.restartTimer1()
            elif k > self.r:
                self.startRound(k)
        else:
            pass    # msg from unknown peer, log something?
