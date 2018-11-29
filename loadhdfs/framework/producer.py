#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import time
import multiprocessing
from utils  import util

class BaseProducer(multiprocessing.Process):
    def __init__(self,msgBus,task_queue,log,identity,framwkconfig):
        multiprocessing.Process.__init__(self)
        self.sys_queue       = msgBus
        self.task_queue      = task_queue
        self.log             = log   
        self.identity        = identity
        self.procWait        = 1
        self.frameworkconfig = framwkconfig
        # runing environment

    def setTaskQueue(self,queue):
        self.task_queue = queue

    def sendMsgToFramework(self,msg):
        self.sys_queue.put(msg)

    def initialize(self,filename):
        if not fileoperation.isFileExist(filename):
            self.log.error("[%s] initialize failed for config file %s no exists", __file__, filename)
            return False
        return self.parseConfig(filename)

    # return name-string
    def getId(self):
        return self.identity

    def generateTask(self):
        pass

    def idle(self):
        pass

    def taskJoin(self):
        # wait the task has be finished
        self.task_queue.join()

    def run(self):
        pass



