#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import os
import time
from framework.scanDir           import Scanner
from framework.producer          import BaseProducer
from framework.baseConfig        import FileSource,ScannerConfig  
from utils.iniConfigParser       import IniConfigParser    
from utils import fileoperation,util

class ScanProducer(BaseProducer):
    def __init__(self,msgBus,task_queue,log,name,framwkconfig):
        BaseProducer.__init__(self,msgBus,task_queue,log,name,framwkconfig)
        self.scanner          = Scanner(log)
        self.fileSource = None
        self.scannerConfig    = None
        self.procSpeed        = 5
        self.consumerNum      = 1

    def setConsumerNum(self,consumers):
        self.consumerNum = consumers

    def setProcSpeed(self,procSpeed):
        pass

    def getFileSource(self):
        return self.fileSource

    def setFileSource(self,filesource):
        self.fileSource = filesource

    def getScannerConfig(self):
        return self.scannerConfig

    def setScannerConfig(self,scancnf):
        self.scannerConfig = scancnf
    
    def initialize(self,filename):
        if not fileoperation.isFileExist(filename):
            self.log.error("[%s] producer(%s) initialize failed for config file %s no exists", __file__, self.getId(), filename)
            return False
        bRet = self.parseConfig(filename)
        if not bRet:
            self.log.error("[%s] producer(%s) initialize failed for config file %s parse failed", __file__, self.getId(), filename)
            return False

        bRet = self.scannerInit()
        if not bRet:
            self.log.error("[%s] producer(%s) initialize failed for scannerInit failed", __file__, self.getId())
            return False
        return True

    def parseConfig(self,filename):
        pass

    def scannerInit(self):
        filesourcelist = self.fileSource.getSourcePath()
        if len(filesourcelist) == 0:
            self.log.error('[%s] producer(%s) not configure file source dir', __file__, self.getId())
            return False

        for fsource in filesourcelist:
            if not fileoperation.isFileExist(fsource):
                self.log.error('[%s] producer(%s) file source dir:%s not exist', __file__, self.getId(), fsource)
                return False

        self.scanner.setMatchPostfix(self.scannerConfig.getPostFix())
        return True

    def idle(self):
        #self.log.info("[%s] timerInterval type:%s", __file__, type(self.scannerConfig.getScanInterval()))
        time.sleep(self.scannerConfig.getScanInterval())