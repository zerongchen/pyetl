#!/usr/bin/env python
# _*_ coding: utf-8 _*_
import os
import sys
import time
sys.path.append(sys.path[0])
from utils import util

def status(proc):
    proc = "etl_controller"
    retval,num = util.executeCMD3("pidof "+proc)
    if retval == 0:
        return True
    return False

def start(proc):
    start_str = "/usr/bin/python " + proc
    os.system(start_str)
    print "START OK"
    return True

def stop(proc):
    proc = "etl_controller"
    if status(proc) is False:
        print "STOP OK"
        return True
    util.executeCMD3("kill `pidof etl_controller`")
    print "TO STOP .",
    sys.stdout.flush()
    while True:
        if not status(proc):
            break
        time.sleep(1)
        print ".",
        sys.stdout.flush()
    print "SUCCESS"
    return True

def pyps(proc):
    start_str = "ps -ef|grep etl_ |grep -v grep"
    os.system(start_str)
    return True

if __name__=="__main__":
    program = "framework/controller.py"
    if sys.argv[1] == "start":
        start(program)
    if sys.argv[1] == "stop": 
        stop(program)
    if sys.argv[1] == "ps":
        pyps(program)
    if sys.argv[1] == "status":
        if status(program):
            print "RUNING"
        else:
            print "STOP"
            
        
