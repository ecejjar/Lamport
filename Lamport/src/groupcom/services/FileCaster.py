'''
Created on 04/03/2013

@author: ecejjar
'''

from collections import namedtuple
from groupcom import server
import json
import os

class FileCaster(server.RMcastServer):
    BeginFileMsg = namedtuple('BeginFileMsg', 'fname, fsize')
    EndFileMsg = namedtuple('EndFileMsg', 'hash')
    ReceiveErrorMsg = namedtuple('ReceiveError', 'reason')
    
    def __init__ ( self, mcast_hostport, hostport, ttl, station_id=None, chunksize=1400 ):
        super(FileCaster, self).__init__(mcast_hostport, hostport, ttl, self, station_id)
        self.__chunksize = chunksize
        self.__sending = None
        self.__receiving = {}

    def send ( self , msg ):
        super(FileCaster, self).send(type(msg).__name__ + ':' + json.dumps(msg.__dict__))
        
    def cast ( self, files ):
        try:
            map(lambda f: FileCaster.cast(self, f), files)
        except TypeError:
            h = 0
            size = os.stat(files).st_size
            f = open(files, 'rb')
            self.__sending = files
            self.send(FileCaster.BeginFileMsg(fname=files, fsize=size))
            while True:
                chunk = f.read(self.__chunksize)
                if chunk is None: break
                super(FileCaster, self).send(chunk)
            self.__sending = None
            self.send(FileCaster.EndFileMsg(hash=h))
            f.close()
    
    def handle ( self, msg, src ):
        msgname, msgval = msg.split(':', 1)
        if msgname in filter(lambda m: m.endswith('Msg'), self.__dir__()):
            MsgType = type(self).__dict__[msgname]
            msg = MsgType(**json.loads(msgval))
            
            if isinstance(msg, FileCaster.BeginFileMsg):
                # By preventing handling of BeginFileMsg from own address we avoid receiving our own files
                if self.server_address[0] == src: return
                
                if src in self.__receiving:
                    self.abort(src)
                try:
                    self.__receiving[src] = open(msg.fname, 'w+b')
                except Exception as e:
                    self.send(\
                        FileCaster.ReceiveErrorMsg(\
                            reason="Unable to open destination file, error: %s" % e.message))
            elif isinstance(msg, FileCaster.EndFileMsg):
                if src in self.__receiving:
                    self.finish(src)
            elif isinstance(msg, FileCaster.ReceiveErrorMsg):
                if self.__sending is not None:
                    pass    # we can't stop sending, other receivers might be OK
            else:
                if src in self.__receiving:
                    try:
                        self.__receiving[src].write(msg)
                    except Exception as e:
                        self.send(\
                            FileCaster.ReceiveErrorMsg(\
                                reason="Unable to write to destination file, error: %s" % e.message))
                        self.abort(src)
            
    def finish ( self, src ):
        f = self.__receiving[src]
        f.close()
        del self.__receiving[src]

    def abort ( self, src ):
        filename = self.__receiving[src]
        self.finish(src)
        os.remove(filename)
        

if __name__ == '__main__':
    pass
