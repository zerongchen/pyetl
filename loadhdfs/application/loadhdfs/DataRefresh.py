#!/usr/bin/env python
#-*- coding:utf-8 -*-

from refreshReq                    import RefreshMsg
from configInfo                    import RefreshConfig 
from framework.log                 import RunningLog,getlogger
from utils                         import util,fileoperation,hdfsOperation
from DataConfigParser               import DataConfigParser 
from framework.consumer            import BaseConsumer
import multiprocessing
import time
import os
import threading

class Refresher(BaseConsumer):
    def __init__(self,msgBus,task_queue,log,name,framwkconfig):
        BaseConsumer.__init__(self,msgBus,task_queue,log,name,framwkconfig)
        self.processInterval  = 180
        self.refreshConfig    = None
        self.filetypesConfig  = None
        self.notifyTabs       = {}


    def initialize(self,filename):
        if not fileoperation.isFileExist(filename):
            self.log.error("[%s] DataRefresh(%s) initialize failed for config file %s no exists", __file__, self.getId(), filename)
            return False
        bRet = self.parseConfig(filename)
        if not bRet:
            self.log.error("[%s] DataRefresh(%s) initialize failed for config file %s parse failed", __file__, self.getId(), filename)
            return False
        
        return self.refreshInit()
    
    def refreshInit(self):
        self.processInterval = self.refreshConfig.getInterval()
        for tabname in self.filetypesConfig.getKeys():
            self.notifyTabs[tabname] = []
        return True

 

    def parseConfig(self,filename):
        try:
            bRet = False
            fileTypesPath  = "/config/FileTypes/FileInfo"
            prefixKey   = None
            subpathKey  = None
            hiveTabKey  = "hiveTab"
            partionKey  = "partitionBy"
            compressKey = "compress"
            refreshKey  = None
            datypeKey   = "type"
            ispartKey   = "ispart"
            DataConfigParser.instance().parse(filename)
            self.refreshConfig   = DataConfigParser.instance().parserRefresh("/config/Refresh/cycle","/config/Refresh/hdfspath","/config/Refresh/postifx")  
            self.filetypesConfig = DataConfigParser.instance().parseFileTypes(fileTypesPath,prefixKey,subpathKey,hiveTabKey,partionKey,compressKey,refreshKey,datypeKey,ispartKey) 
            if None in (self.refreshConfig,self.filetypesConfig):
                return False        
            return True    
        except:
            self.log.error('[%s] DataRefresh(%s) parse config %s failed for error %s', __file__, self.getId(), filename,util.getExceptInfo())
            return False 

    def refresh(self):
        msgNum = self.task_queue.qsize()
        if msgNum == 0:
            return True

        for x in xrange(msgNum):
            msg = self.task_queue.get()
            self.task_queue.task_done() 
            tabname  = msg.getTabname()
            if self.notifyTabs.has_key(tabname):
                self.notifyTabs[tabname].append(msg.getfilename())
            else:
                self.log.error("[%s] DataRefresh(%s) unknown hive tabname:%s, please add it to loaddata.xml", __file__, self.getId(), tabname)
                return False

        for key in self.notifyTabs.keys():
            if len(self.notifyTabs[key]) > 0:
                if self.refreshRecord(key,self.notifyTabs[key]):
                   self.notifyTabs[key] = []
                else:
                    self.log.error("[%s] DataRefresh(%s) refreshRecord(%s) failed", __file__, self.getId(),key)
        return True

    def refreshRecord(self,tabname,filenameslist):
        localIP   = util.getLocalIP()
        timestamp = int(time.time())
        records   = "\n".join(filenameslist) + "\n"
        filename  = "%s_%s.%s" % (localIP,timestamp,self.refreshConfig.getPostFix())

        self.log.info("[%s] DataRefresh(%s) process msg %s", __file__, self.getId(), msg.toString())

        if not fileoperation.openAndWrietData(filename,records):
            self.log.error("[%s] DataRefresh(%s) open and write %s failed", __file__, self.getId(),filename)
            return False

        hdfspath = os.path.join(self.refreshConfig.getStorePath(), tabname)
        if not hdfsOperation.storeFileToHadoop(filename,hdfspath, self.log):
            self.log.error("[%s] DataRefresh(%s) storeFileToHadoop(%s,%s) failed", __file__, self.getId(),filename,hdfspath)
            return False

        self.log.info("[%s] DataRefresh(%s) refresh success, hivetab:%s", __file__, self.getId(),tabname)
        return True

    def idle(self):
        self.log.debug("[%s] timerInterval type:%s", __file__, type(self.processInterval))
        time.sleep(self.processInterval)

    def ExitNotify(self):
        if self.sys_queue.empty():
            return False
        task = self.sys_queue.get()
        self.sys_queue.task_done()
        if task == self.name + "exit":
            return True
        else:
            self.sys_queue.put(task)
            return False

    def run(self):
        util.setProcessName(self.getId())
        self.log.info("[%s] DataRefresh(%s) pid is %d to run", __file__, self.getId(),os.getpid())
        while True:
            try:
                if self.ExitNotify():
                    break
                self.refresh()
                self.idle()
            except:
                self.log.error('[%s] DataRefresh(%s) pid is %d to run error(%s)', __file__, self.getId(),os.getpid(), util.getExceptInfo())
                time.sleep(self.processInterval)
        self.log.info("[%s] DataRefresh(%s) pid is %d to normal exit", __file__, self.getId(), os.getpid())