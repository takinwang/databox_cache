#
import os, sys
import random 

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

import time, datetime
import zlib, json 

if PY2 :
    import Queue as queue
else :
    import queue 
     
import socket 

from struct import unpack, pack, pack_into

'''
    The remaining chars indicate types of args and must match exactly;
    these can be preceded by a decimal repeat count:
      x: pad byte (no data); c:char; b:signed byte; B:unsigned byte;
      ?: _Bool (requires C99; if not available, char is used instead)
      h:short; H:unsigned short; i:int; I:unsigned int;
      l:long; L:unsigned long; f:float; d:double.
    Special cases (preceding decimal count indicates length):
      s:string (array of char); p: pascal string (with count byte).
    Special cases (only available in native format):
      n:ssize_t; N:size_t;
      P:an integer type that is wide enough to hold a pointer.
    Special case (not in native mode unless 'long long' in platform C):
      q:long long; Q:unsigned long long
    Whitespace between formats is ignored.
'''

HBA_TAG = 19790616


class PyHiveData(object):

    def __init__(self):
        self.data_write = [ ] 
        self.set_data(b"")
        
    def read_uint8(self, _def):
        if self.read_indx + 1 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx: self.read_indx + 1 ]
        self.read_indx = self.read_indx + 1
        return unpack("B", v)[0]
        
    def write_uint8(self, v):
        self.data_write.append(pack("B", v))
        
    def read_int8(self, _def):
        if self.read_indx + 1 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx: self.read_indx + 1 ]
        self.read_indx = self.read_indx + 1
        return unpack("b", v)[0]
        
    def write_int8(self, v):
        self.data_write.append(pack("b", v))
    
    def read_uint16(self, _def):
        if self.read_indx + 2 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 2 ]
        self.read_indx = self.read_indx + 2
        return unpack("H", v)[0]
    
    def write_uint16(self, v):
        self.data_write.append(pack("H", v))

    def read_int16(self, _def):
        if self.read_indx + 2 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 2 ]
        self.read_indx = self.read_indx + 2
        return unpack("h", v)[0]
    
    def write_int16(self, v):
        self.data_write.append(pack("h", v))

    def read_uint32(self, _def):
        if self.read_indx + 4 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 4 ]
        self.read_indx = self.read_indx + 4
        return unpack("I", v)[0]

    def write_uint32(self, v):
        self.data_write.append(pack("I", v))
        
    def read_int32(self, _def):
        if self.read_indx + 4 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 4 ]
        self.read_indx = self.read_indx + 4
        return unpack("i", v)[0]

    def write_int32(self, v):
        self.data_write.append(pack("i", v))
        
    def read_uint64(self, _def):
        if self.read_indx + 8 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 8 ]
        self.read_indx = self.read_indx + 8
        return unpack("L", v)[0]
        
    def write_uint64(self, v):
        self.data_write.append(pack("L", v))

    def read_int64(self, _def):
        if self.read_indx + 8 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 8 ]
        self.read_indx = self.read_indx + 8
        return unpack("l", v)[0]
        
    def write_int64(self, v):
        self.data_write.append(pack("l", v))
        
    def read_float32(self, _def):
        if self.read_indx + 4 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 4 ]
        self.read_indx = self.read_indx + 4
        return unpack("f", v)[0]
        
    def write_float32(self, v):
        self.data_write.append(pack("f", v))
        
    def read_float64(self, _def):
        if self.read_indx + 8 > self.data_leng:
            return _def 
        
        v = self.data_read[ self.read_indx : self.read_indx + 8 ]
        self.read_indx = self.read_indx + 8
        return unpack("d", v)[0]
        
    def write_float64(self, v):
        self.data_write.append(pack("d", v))
        
    def read_str(self, _def):
        leng = self.read_uint64(None)
        if leng is None: 
            return _def         
        if self.read_indx + leng > self.data_leng:
            return _def         
        v = self.data_read[ self.read_indx : self.read_indx + leng ]
        self.read_indx = self.read_indx + leng
        return v
        
    def write_str(self, v):
        v = decode_to_bytes(v)
        self.write_uint64(len(v))
        self.data_write.append(v)
        
    def pack_data(self):
        t = []
        t.append(pack("I", HBA_TAG))
        d = b"".join(self.data_write)
        t.append(pack("I", len(d)))
        t.append(d)
        return b"".join(t)
    
    def get_data(self):
        return b"".join(self.data_write)
    
    def set_data(self, v):
        self.data_read = decode_to_bytes(v)        
        self.read_indx = 0
        self.data_leng = len(self.data_read)    

class Connection(object):

    def __init__(self, hostinfo, timeout=10):          
        self.hostinfo = hostinfo 
        self.timeout = timeout 
        
#     def __del__(self):
#         print("__del__", id(self))
                
    def connect(self): 
        host, port = self.hostinfo.split(":")
        self.c = socket.create_connection([ host , int(port) ], timeout=self.timeout) 
        self.c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.c.setsockopt(socket.SOL_SOCKET, socket. SO_KEEPALIVE, 1)
        return self 
                
    def _recv_action(self): 
        while True: 
            cvdata = self._recv(4 + 4 + 1)              
            tag, size, action = unpack("IIB", cvdata)
            if tag != HBA_TAG:
                raise DBoxException(tsFailed, b"Invalid data structure")                      
            return size, action 
    
    def commuicate(self, data):    
        data = data.pack_data()             
        self.c.sendall(data)                
        size, action = self._recv_action()                
        data = self._recv(size - 1)
        return action, data           

    def _recv(self, size):
        if size == 0:
            return b"" 
        
        to_recv = size
        buffer = [ ] 
        while to_recv > 0:
            data = self.c.recv(to_recv)
            lsize = len(data) 
            if lsize == 0:
                break 
            to_recv -= lsize
            buffer.append(data) 
        data = b"".join(buffer)
        
        if len(data) != size:
            msg = "Size not equal: %s--%s" % (len(data) , size)
            raise DBoxException(tsFailed, msg.encode('utf_8'))  
        return data 


def encode_to_str(name):
    if name is None:
        return None 
    
    if isinstance(name, bytes):
        return name.decode("utf-8")
    
    return str(name)


def decode_to_bytes(name):
    if name is None:
        return bytes() 
      
    if isinstance(name, bytes):
        return name
  
    if PY2 :
        t = isinstance(name, (str, unicode))
    else:
        t = isinstance(name, str)
        
    if not t :
        raise TypeError("'name' arg must be a byte string")
  
    encoding = sys.getfilesystemencoding() or 'utf-8'
    try:
        return name.encode(encoding)
    except UnicodeEncodeError as exc:
        raise ValueError(
            "Cannot convert 'name' to a bytes name: %s" % exc)
 
