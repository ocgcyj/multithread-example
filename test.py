# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 17:54:59 2018

@author: YINGJIE CHEN
"""

import threading
import time
import simplejson
import sys
import _thread

plist = []

def func():
    for i in range(5):
        plist.append(i)
        print(plist)
        time.sleep(2)
        
    _thread.interrupt_main() 
    while 1:
        continue

def timer_func():
    print(plist)
    timer = threading.Timer(1, timer_func)
    timer.setDaemon(True)
    timer.start()
#    _thread.interrupt_main()  
if __name__ == '__main__':
    
    thread = threading.Thread(target=func, args=())
    
    try:
        thread.setDaemon(True)
        thread.start()
        timer_func()
        
        
        while True: 
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("main thread exit")
        
    finally:
        print("save data")
        f = open('output.txt', 'w')
        simplejson.dump(plist, f)
        f.close()
    
    
    


    