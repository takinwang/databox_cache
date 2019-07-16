import threading
import time
from dboxslab_rpc import CacheClient

import sys
if __name__ == "__main__":
    client0 = CacheClient("10.0.81.2:6501", 120)   
    client1 = CacheClient("10.0.81.3:6501", 120)
    
#     fname = "mem:///tmp/abc.txt"
#     txt = client1.Open(fname).Read(10000) 
#     print(client1.hostinfo, txt)  

    for idx in range(128, 130) :   
        fname = "mem:///test_%s.txt" % idx        
        ofile = client0.Open(fname, False)
        print(ofile.Write(fname))  
 
#     for no in range(30):
#     
#         for idx in range(128, 132) :
#             fname = "local:///test_%s.txt" % idx   
#             txt = client0.Open(fname).Read(10000) 
#             print(client0.hostinfo, txt)          
#     
#         for idx in range(128, 132) :
#             fname = "local:///test_%s.txt" % idx   
#             txt = client1.Open(fname).Read(10000) 
#             print(client1.hostinfo, txt)   
         
    for idx in range(128, 130) :   
        fname = "mem:///test_%s.txt" % idx        
        ofile = client1.Open(fname, False)
        print(ofile.Read(10000))
            
    time.sleep(1)
 
    txt = client1.Open("rados:///c1/p0/L5-TM-122-031-19840419-LSR.BANDS", False).Read(100000)
    print(txt)
 
    for idx in range(128, 130) :
        fname = "mem:///test_%s.txt" % idx  
        client0.Unlink(fname) 
