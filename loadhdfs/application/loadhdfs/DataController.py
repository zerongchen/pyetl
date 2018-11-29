#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import DataFileProcess         
import DataFileScan  
import DataRefresh      
from   utils import util
from   framework.controller import TaskController

class DataProcessController(TaskController):
    def __init__(self,frameworkconfig,logname):
        TaskController.__init__(self,frameworkconfig,logname)

    def toplogyInit(self):
        bRet = False
        try:           
            taskProducerName = "DataFileScanner"
            taskConsumerName = "DataFileProcess"
            taskRefresherName = "DataRefresh"
            taskConsumerCls = getattr(DataFileProcess,"DataFileStoreHadoop")
            taskProducerCls = getattr(DataFileScan,"DataFileScan")
            taskRefresherCls = getattr(DataRefresh,"Refresher")

            loaddataFileName = os.path.join(self.frameworkconfig.confDir, "loaddata.xml")
            self.log.info("[%s] producer & consumer configure file: %s", __file__, loaddataFileName)

            # DataFileScanner
            self.log.info("[%s] to create %s producer(%s) ", __file__, 1,taskProducerName)
            if not self.producerInit(taskProducerName,taskProducerCls,1,loaddataFileName):
                self.log.error("[%s] producer %s init failed", __file__, taskProducerName)
                return False

            # DataFileProcess
            self.log.info("[%s] to create %s consumer(%s) ", __file__, str(self.processnum), taskConsumerName)
            if not self.consumerInit(taskConsumerName,taskConsumerCls,self.processnum,loaddataFileName):
                self.log.error("[%s] consumer %s init failed", __file__, taskConsumerName)
                return False

            # DataFileScanner -> task queue of DataFileProcess
            self.bindProducerConsumer(taskProducerName, taskConsumerName, self.processnum)

            # DataRefresh
            if not self.consumerInit(taskRefresherName,taskRefresherCls,1,loaddataFileName):
                self.log.error("[%s] consumer %s init failed", __file__, taskRefresherName)
                return False

            # DataFileProcess -> task queue of DataRefresh
            self.bindProducerConsumer(taskConsumerName,taskRefresherName,0)

            self.log.info("[%s] consumer init ok consumer num:%d", __file__, len(self.consumers))
            self.log.info("[%s] producer Init ok producer num:%d", __file__, len(self.producers.values()))
            self.log.info("[%s] toplogyInit success", __file__)
            return True
        except:
            self.log.error("[%s] toplogyInit failed for %s", __file__, util.getExceptInfo())
            return False
        
        
