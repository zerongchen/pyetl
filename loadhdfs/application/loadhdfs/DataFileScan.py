#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from framework.scanProducer   import ScanProducer
from DataConfigParser        import DataConfigParser
from framework.baseConfig     import FileSource,ScannerConfig
from DataFileStoreTask     import DataFileStoreTask
from utils                    import util

class DataFileScan(ScanProducer):
    def __init__(self,msgBus,task_queue,log,name,framwkconfig):
        ScanProducer.__init__(self,msgBus,task_queue,log,name,framwkconfig)

    # override
    def parseConfig(self,filename):
        try:
            bRet = False
            # parser loaddata.xml
            DataConfigParser.instance().parse(filename)         
            scanConfig            = DataConfigParser.instance().parseScannerConfig("/config/Scanner/Interval","/config/Scanner/PostFix")
            self.setFileSource(DataConfigParser.instance().parseFileSource("/config/FileSource/RootPath"))
            scanInterval          = int(scanConfig.getScanInterval())
            scanMatchPostFix      = scanConfig.getPostFix()
            scanDepth             = 1
            self.setScannerConfig(ScannerConfig(scanInterval,scanMatchPostFix,"",scanDepth))
            self.log.info('[%s] DataFileScan(%s) source: %s, scan: %s ', __file__, self.getId(),
                          self.getFileSource().toString(), self.getScannerConfig().toString())
            return True    
        except:
            self.log.error('[%s] DataFileScan(%s) parse config %s failed for error %s', __file__, self.getId(), filename,util.getExceptInfo())
            return False 

    def generateTask(self):
        dealfileflag = False
        filesourcelist = self.getFileSource().getSourcePath()
        for fsource in filesourcelist:
            # get all files
            files = self.scanner.getfiles(fsource)
            if files is None or len(files) == 0:
                continue
            dealfileflag = True
            self.log.info("[%s] DataFileScan(%s) get files %d from %s", __file__, self.getId(), len(files), fsource)
            for fileWithPath in files:
                obj = DataFileStoreTask(self.getId(),"", os.path.join(fsource, fileWithPath))
                self.task_queue.put(obj)
        return dealfileflag

    def ExitNotify(self):
        if self.sys_queue.empty():
            return False
        task = self.sys_queue.get()
        self.sys_queue.task_done()
        if task == self.name+"exit":
            return True
        else:
            self.sys_queue.put(task)
            return False

    def run(self):
        util.setProcessName(self.getId())
        self.log.info("[%s] DataFileScan(%s) pid is %d to run", __file__, self.getId(),os.getpid())
        while True:
            try:
                if not self.generateTask():
                    self.idle()
                else:
                    #time.sleep(self.procWait)
                    self.taskJoin()

                if self.ExitNotify():
                    break
            except:
                self.log.error("[%s] DataFileScan(%s) pid is %d to run error(%s)", __file__, self.getId(),os.getpid(),util.getExceptInfo())
        self.log.info("[%s] DataFileScan(%s) pid is %d to normal exit", __file__, self.getId(), os.getpid())