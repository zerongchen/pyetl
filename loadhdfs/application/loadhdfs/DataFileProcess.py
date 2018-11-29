#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import random
import time
import string
from DataFileStoreTask import DataFileStoreTask
from DataConfigParser import DataConfigParser
from refreshReq import RefreshMsg
from utils import fileoperation,util,hdfsOperation
from framework.consumer         import BaseConsumer

class DataFileStoreHadoop(BaseConsumer):
    def __init__(self,msgBus,task_queue,log,name,framwkconfig):
        BaseConsumer.__init__(self,msgBus,task_queue,log,name,framwkconfig)
        self.hiveAccConfig   = None
        self.fileTypeConfig  = None
        self.currentFileType = None
        self.processFileCount = int(0)
        self.processRealTimeCount = float(0.0)
        self.processUserTimeCount = float(0.0)
        self.processCompressTimeCount = float(0.0)
        self.STATISCYCLESECS = float(300.0)
        #self.refresher       = refresher
        # runing environment

    def initialize(self,filename):
        if not fileoperation.isFileExist(filename):
            self.log.error("[%s] DataFileProcess(%s) config file %s no exists", __file__, self.getId(), filename)
            return False
        return self.parseConfig(filename)


    def parseConfig(self,configFilename):
        try:
            fileTypesPath  = "/config/FileTypes/FileInfo"
            prefixKey  = "prefix"
            subpathKey = None
            hiveTabKey = "hiveTab"
            partionKey = "partitionBy"
            compressKey= "compress"
            refreshKey = "refresh"
            datypeKey  = "type"
            ispartKey  = "ispart"
            DataConfigParser.instance().parse(configFilename)
            self.fileTypeConfig   = DataConfigParser.instance().parseFileTypes(fileTypesPath,prefixKey,subpathKey,hiveTabKey,partionKey,compressKey,refreshKey,datypeKey,ispartKey)
            self.hiveAccConfig    = DataConfigParser.instance().parseHiveAccConfig("/config/storeconfig/bin","/config/storeconfig/Warehouse")
            if None in (self.fileTypeConfig,self.hiveAccConfig):
                self.log.error("[%s] DataFileProcess(%s) parse config %s failed", __file__, self.getId(), configFilename)
                return False
            return True
        except:
            self.log.error('[%s] DataFileProcess(%s) parse config %s failed for error %s', __file__, self.getId(), configFilename,util.getExceptInfo())
            return False 

    
    def getRefreshFlag(self,prefix):        
        self.currentFileType = self.fileTypeConfig.getFileType(prefix)
        return self.currentFileType.IsNeedRefresh()
        #return True

    def getbPartFlag(self,prefix):
        self.currentFileType = self.fileTypeConfig.getFileType(prefix)
        return self.currentFileType.IsNeedPart()

    def refreshMetaNotify(self,prefix,filenameWithPath):
        if self.getRefreshFlag(prefix):
            msg = RefreshMsg(self.getId(),"",prefix,filenameWithPath)
            self.sendMsgToFramework(msg)
            return True
        else:
            return True

    def getHiveTabname(self,prefix):
        self.currentFileType = self.fileTypeConfig.getFileType(prefix)
        return self.currentFileType.getHiveTabName()
        
    def getPartitionDateStr(self,prefix,reportTimeSeconds):
        self.currentFileType = self.fileTypeConfig.getFileType(prefix)
        partitionKey  = self.currentFileType.getPartitionKey()
        if partitionKey == "hour":
            return reportTimeSeconds[0:10]
        else:
            return reportTimeSeconds[0:8]

    def getDatabaseName(self):
        pathTmp = self.hiveAccConfig.getWareHouse().split('/')
        pathLast = pathTmp[-1]
        if(".db" in pathLast):
            return pathLast
        else:
            return 'default'

    def getHivePath(self):
        return os.path.join(self.hiveAccConfig.getHivePath(),"hive")

    def getPartitionPath(self,hiveTabname,areaid,dateStr):
        return os.path.join(self.hiveAccConfig.getWareHouse(),hiveTabname,areaid,dateStr)


    def getHdfsPath(self,tabname,filenameWithPath):
        pos = filenameWithPath.find(tabname)
        if pos == -1:
            self.log.error('[%s] DataFileProcess(%s) process invalid filename:%s', __file__, self.getId(),filenameWithPath)
            return None
        postfix_path = filenameWithPath[pos:]
        wareHouse    = self.hiveAccConfig.getWareHouse()
        complete_hdfspath = os.path.join(wareHouse,postfix_path)
        return os.path.dirname(complete_hdfspath)

    def getCompressFlag(self,prefix):
        self.currentFileType = self.fileTypeConfig.getFileType(prefix)
        return self.currentFileType.IsCompress()
    
    def compressFilename(self,srcfilename,prefix):        
        if self.getCompressFlag(prefix):
            if fileoperation.compresessFile(srcfilename,os.path.dirname(srcfilename),self.log):
                return srcfilename + ".lzo"
            else:
                return None
        else:
            return srcfilename

    def getPartition(self,prefix,areaid,reportTimeSeconds):
        dateStr = self.getPartitionDateStr(prefix,reportTimeSeconds)
        tabnamestr = self.getHiveTabname(prefix)
        if prefix in ("originalbill,originalbillother,originalbillgroup,originalbillhome,HTTP,NoneHttp"):
            hourStr = reportTimeSeconds[8:10]
            partitionDir = os.path.join(self.hiveAccConfig.getWareHouse(),tabnamestr,areaid,dateStr,hourStr)
        else:
            partitionDir = os.path.join(self.hiveAccConfig.getWareHouse(),tabnamestr,areaid,dateStr)
        return partitionDir

    def getTransFileName(self,srcname):
        tmp = srcname.split('/')
        return tmp[-1]

    #prefix areaid reporttime
    #post file name: shenzhen_dg8_01_20141218154400.txt
    def getFileInfo(self,filepath,basename):
        #self.log.info("filepath:%s",filepath)
        if("/post/" in filepath):
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = 'post'
            reportTimeSeconds = tmp[-1].split('.')[0]
        elif("/postsuspect/" in filepath):
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = 'postsuspect'
            reportTimeSeconds = tmp[-1].split('.')[0]
        elif("/radius/" in filepath):
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = tmp[1]
            reportTimeSeconds = tmp[-1].split('.')[0]
        elif("/http/" in filepath):
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = 'http'
            reportTimeSeconds = tmp[2]
        elif("/wap/" in filepath):
            tmp = basename.split('_')
            areaid = filepath.split('/')[-2]
            prefix = 'wap'
            reportTimeSeconds = tmp[-1].split('.')[0]
        elif("/areanetwork/" in filepath):
            tmp = basename.split('_')
            areaid = filepath.split('/')[-2]
            prefix = 'areanetwork'
            reportTimeSeconds = tmp[-1].split('.')[0]
        elif("/intelligentpush/" in filepath):
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = tmp[1]
            reportTimeSeconds = tmp[-1].split('.')[0]
        elif("/appuser/" in filepath):
            tmp = basename.split('_')
            areaid = 'guangdong'
            prefix = tmp[0]
            tmptime =  tmp[-1].split('.')[0]
            tmptime2=string.atof(tmptime)
            timecal = time.localtime(tmptime2)
            reportTimeSeconds = time.strftime('%Y%m%d',timecal)
        elif("/phone/" in filepath):
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = tmp[1]
            reportTimeSeconds = tmp[3]
        else:
            tmp = basename.split('_')
            areaid = tmp[0]
            prefix = tmp[1]
            reportTimeSeconds = tmp[2]
        return prefix,areaid,reportTimeSeconds

    # to deal every file
    def processTask(self,task):
        try:
            processId = self.getId()
            filename  = task.getfilename()
            if not fileoperation.isFileExist(filename):
                self.log.error("[%s] DataFileProcess(%s) check: %s no exist", __file__, processId,filename)
                return False
            
            basename = task.getBasename()
            prefix,areaid,reportTimeSeconds = self.getFileInfo(filename,basename)

            wall_compress_t0 = float(0.0)
            wall_compress_t1 = float(0.0)
            srcDir = filename
            if basename.split('.')[-1] != "lzo":
                # measure wall time
                wall_compress_t0 = time.time()
                srcDir  = self.compressFilename(filename,prefix)
                if srcDir is None:
                    self.log.error('[%s] DataFileProcess(%s) compress %s failed', __file__, processId, filename)
                    return False
                wall_compress_t1 = time.time()

            partitionDir = self.getPartition(prefix, areaid, reportTimeSeconds)
            if not hdfsOperation.storeFileToHadoop(srcDir, partitionDir, self.log):
                time.sleep(1)
                return False

            self.processCompressTimeCount += wall_compress_t1 - wall_compress_t0

            tmpname = os.path.basename(srcDir)
            self.refreshMetaNotify(prefix,os.path.join(partitionDir,tmpname))
            self.log.info("[%s] DataFileProcess(%s) put %s to hadoop %s success", __file__, self.getId(), srcDir, partitionDir)
            return True
        except:
            self.log.error('[%s] DataFileProcess(%s) process task %s error(%s)', __file__, processId, task.toString(), util.getExceptInfo())
            return False

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

    # process loop deal
    def run(self):
        util.setProcessName(self.getId())
        self.log.info("[%s] DataFileProcess(%s) pid is %d to run", __file__, self.getId(), os.getpid())
        while True:
            try:
                if self.ExitNotify():
                    break

                if self.task_queue.empty():
                    time.sleep(1)
                    continue

                task = self.task_queue.get()
                self.log.info('')
                self.log.info('[%s] DataFileProcess(%s) process task %s', __file__, self.getId(), task.toString())

                # measure process time
                process_t0 = time.clock()
                # measure wall time
                wall_t0 = time.time()
                if not self.processTask(task):
                    # failed process
                    self.log.info("[%s] DataFileProcess(%s) process task %s failed", __file__, self.getId(), task.toString())
                else:
                    # success process
                    self.processFileCount += 1
                    self.processRealTimeCount += time.time() - wall_t0
                    self.processUserTimeCount += time.clock() - process_t0
                    if self.processRealTimeCount > self.STATISCYCLESECS:
                        self.log.info("[%s] DataFileProcess(%s) times statis: { FileCount: %d, RealTime: %.3f, UserTime: %.3f, CompressTime: %.3f }", __file__, self.getId(),
                                      self.processFileCount, self.processRealTimeCount, self.processUserTimeCount, self.processCompressTimeCount)
                        self.processFileCount = 0
                        self.processRealTimeCount = float(0.0)
                        self.processUserTimeCount = float(0.0)
                        self.processCompressTimeCount = float(0.0)

                self.task_queue.task_done()

            except:
                self.log.error('[%s] DataFileProcess(%s) pid is %d to run error(%s)' , __file__, self.getId(), os.getpid(), util.getExceptInfo())
                time.sleep(1)
        self.log.info("[%s] DataFileProcess(%s) pid is %d to normal exit", __file__, self.getId(), os.getpid())