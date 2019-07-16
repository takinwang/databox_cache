# encoding: utf-8
import os, sys
import random 

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

import time, datetime
import zlib, json 
  
from pysock import PyHiveData, Connection, encode_to_str, decode_to_bytes


class DBoxException(Exception):

    def __init__(self, state , message):
        self.state = state 
        self.message = encode_to_str(message) 
        
    def __str__(self):
        return "%s, state: %d" % (self.message, self.state)


tsFailed = -1000              # 失败
tsSuccess = 0                 # 成功

caSuccess = 0                 # 通用返回
caOpen = 1                    # 打开缓存文件  
caClose = 2                   # 关闭缓存文件

caAdmin = 8                   # 远程控制
caAdminResp = 9               # 远程控制返回
    
caClientRead = 10             # 读取缓存数据
caClientReadResp = 11         # 读取数据返回

caClientWrite = 12            # 写入数据到缓存
caClientWriteResp = 13        # 写入数据返回

caClientUnlink = 14           # 客户端删除文件
caClientUnlinkResp = 15       # 客户端删除文件返回

caClientMkDir = 16            # 客户端建立目录
caClientMkDirResp = 17        # 客户端建立目录返回

caClientRmDir = 18            # 客户端删除目录
caClientRmDirResp = 19        # 客户端删除目录返回

caClientTruncate = 20         # 客户端截取文件
caClientTruncateResp = 21     # 客户端截取文件返回

caClientGetAttr = 22          # 客户端读取文件属性
caClientGetAttrResp = 23      # 客户端读取文件属性返回

caClientFlush = 24            # 客户端刷新写入数据
caClientFlushResp = 25        # 客户端刷新写入数据返回

FAILED_INVALID_RESPONSE = "Invalid response";
FAILED_CONNECTION_TIMEOUT = "Connection timeout";
FAILED_CONNECTION_FAILED = "Connection failed";


class CacheFile(object):
    
    def __init__(self, hostinfo, filename, readonly, timeout=10): 
        self.hostinfo = hostinfo
        self.filename = filename 
        self.readonly = readonly  
        self.timeout = timeout
        
    def Read(self, size , offset=0):         
        pydata = PyHiveData()
        pydata.write_int8(caClientRead)
        
        pydata.write_str(self.filename) 
        pydata.write_int8(1 if self.readonly == True else  0)
        
        pydata.write_int64(offset);
        pydata.write_uint64(size);               

        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientReadResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
                     
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if state != tsSuccess :
            raise DBoxException(state, message)
        
        return message
        
    def Write(self, data, offset=0, async_write=True):        
        if self.readonly == True:
            raise DBoxException(tsFailed, "ReadOnly") 
        
        if len(data) == 0:
            return 0 
             
        pydata = PyHiveData()
        pydata.write_int8(caClientWrite)
        
        pydata.write_str(self.filename) 
        pydata.write_int8(1 if self.readonly == True else  0)
        
        pydata.write_int64(offset);
        pydata.write_str(data);   
        pydata.write_int8(1 if async_write == True else 0)     
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientWriteResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        bytes = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if bytes < 0:
            raise DBoxException(bytes, message)
        
        return bytes
    
    def Flush(self):
        '''
        刷新远程块缓存服务器的修改块 和 后端存储 
        '''
        pydata = PyHiveData()
        pydata.write_int8(caClientFlush)
         
        pydata.write_str(self.filename)  
                
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
         
        if action != caClientFlushResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
         
        pydata.set_data(data)     
         
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
         
        if state != tsSuccess:
            raise DBoxException(state, message)
         
        return state    

    
class CacheClient(object):

    def __init__(self, hostinfo, timeout=10):
        '''
        hostinfo  "127.0.0.1:8.61，采用 TCP 连接 
        '''
        self.hostinfo = encode_to_str(hostinfo) 
        self.timeout = timeout
        
    def Open(self, filename, readonly=True):
        '''
        新建一个文件对象进行读写
        当 readonly == true  模式，由于使用属性（size、version等）缓存优化 3s，读取效率更高，但当其他块缓存服务器上写入数据的后端存储文件，被当前节点在写入之前已经访问，可能会存在数据不同步问题
        当 readonly == false 无数据不同步问题
        或则先 Close 当前块缓存服务器上 filename 的缓存，再访问
        '''
        return CacheFile(self.hostinfo, filename, readonly, timeout=self.timeout)
        
    def Close(self, filename):      
        '''
        关闭远程块缓存服务器上的 filename 缓存信息，再次访问会从邻居或后端存储重新加载
        对于 mem:// 文件，无效
        '''  
        pydata = PyHiveData()
        pydata.write_int8(caClose)
        
        pydata.write_str(filename) 
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caSuccess :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if state != tsSuccess:
            raise DBoxException(state, message)
        
        return state     
        
    def MkDir(self, filename):  
        '''
        在后端存储上建立目录 filename，不同后端存储不一定都实现
        '''      
        pydata = PyHiveData()
        pydata.write_int8(caClientMkDir)
        
        pydata.write_str(filename) 
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientMkDirResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if state != tsSuccess:
            raise DBoxException(state, message)
        
        return state

    def RmDir(self, filename):    
        '''
        在后端存储上删除目录 filename，不同后端存储不一定都实现
        '''      
        pydata = PyHiveData()
        pydata.write_int8(caClientRmDir)
        
        pydata.write_str(filename) 
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientRmDirResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if state != tsSuccess:
            raise DBoxException(state, message)
        
        return state  
         
    def Unlink(self, filename):  
        '''
        在远程块缓存服务器 和 后端存储上删除文件，不同后端存储不一定都实现，其他远程块缓存服务器可能会存在60s的缓存时间
        '''                
        pydata = PyHiveData()
        pydata.write_int8(caClientUnlink)
        
        pydata.write_str(filename) 
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientUnlinkResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if state != tsSuccess:
            raise DBoxException(state, message)
        
        return state    

    def Truncate(self, filename, newsize): 
        '''
        在 后端存储上截取文件，不同后端存储不一定都实现，其他远程块缓存服务器可能会存在60s的缓存时间
        对于 mem:// 文件，暂时未实现
        '''  
        pydata = PyHiveData()
        pydata.write_int8(caClientTruncate)
        
        pydata.write_str(filename) 
        pydata.write_int64(newsize) 
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientTruncateResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed) 
        message = pydata.read_str(FAILED_INVALID_RESPONSE)
        
        if state != tsSuccess:
            raise DBoxException(state, message)
        
        return state    
    
    def GetAttr(self, filename,): 
        '''
        在远程块缓存服务器 和 后端存储上文件属性（ mtime 和 size ），可能会存在60s的缓存时间
        '''  
        pydata = PyHiveData()
        pydata.write_int8(caClientGetAttr)
        
        pydata.write_str(filename)  
               
        c = Connection(self.hostinfo, self.timeout).connect()         
        action, data = c.commuicate(pydata)
        
        if action != caClientGetAttrResp :
            raise DBoxException(tsFailed, FAILED_INVALID_RESPONSE)   
        
        pydata.set_data(data)     
        
        state = pydata.read_int32(tsFailed)      
        message = pydata.read_str(FAILED_INVALID_RESPONSE)   
        
        if state != tsSuccess:
            raise DBoxException(state, message)
        
        return unpack("LL", message)    
    
