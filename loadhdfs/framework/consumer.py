#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import multiprocessing
import time
from utils import util,fileoperation

class BaseConsumer(multiprocessing.Process):
    def __init__(self,msgBus,task_queue,log,name,framwkconfig):
        multiprocessing.Process.__init__(self)
        self.sys_queue  = msgBus
        self.task_queue = task_queue
        self.log        = log   
        self.identity   = name
        self.frameworkconfig = framwkconfig
        #self.currentTask= None

    def sendMsgToFramework(self,msg):
        self.sys_queue.put(msg)

    def initialize(self,filename):
        pass

    def parseConfig(self,filename):
        pass

    def getTask(self): 
        task = None
        try:
            task = self.task_queue.get(block=False)
        except:
            task = None
        return task 

    def getId(self):
        return self.identity

    def run(self):
        pass






