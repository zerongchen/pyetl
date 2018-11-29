#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from  utils import util
from  framework.log import getlogger

class TimerDesc(object):
    def __init__(self,ip,timeout,lastacctime):
        self.ip          = ip
        self.timeout     = timeout              #second
        self.lastacctime = lastacctime
        self.leftTime    = timeout
        self.repeart     = True

class TimePointDesc(object):
    def __init__(self,ctx,timepoint,lastacctime):
        self.ctx         = ctx
        self.timepoint   = timepoint            #datetime
        self.lastacctime = lastacctime
        self.repeart     = True
        self.formatStr   = "%H:%M:%S"
        self.formatTime  = time.strptime(self.timepoint,self.formatStr)

    def getCtxInfo(self):
        return self.ctx

class TimerManager(object):
    def __init__(self):
        self.timerDescList     = []
        self.timePointList     = []
        self.timerBackMap      = {}
        self.log               = getlogger()

    
    # according to timerGranularity to generate timer to shootTimer
    def getShootTimer(self,timerGranularity):
        #first decrease the timeout in timerDesc
        timeval = time.localtime()
        timerShoot = []
        for index ,timer in enumerate(self.timerDescList):
            if timer.leftTime  < timerGranularity:
                timerShoot.append(timer)
                self.timerDescList[index].leftTime  = timer.timeout
            else:
                self.timerDescList[index].leftTime -= timer.timeout

        for index,timer in enumerate(self.timePointList):
            if timeval.tm_hour == timer.formatTime.tm_hour and timeval.tm_min == timer.formatTime.tm_min:
                timerShoot.append(timer)
        #another is for the timepoint timer
        return timerShoot

    def timerStop(self):
        self.timerDescList = []
        self.timePointList = []

    def removeTimer(self,idx,timerType):
        pass
    
    def addTimer(self,timertype,timepoint,timeout,ctx):
        if timertype =="period":
            timer   = TimerDesc(ctx,int(timeout),time.time())
            self.timerDescList.append(timer)
            #self.timerBackMap[ctx] = timer
        else:
            timer   = TimePointDesc(ctx,timepoint,time.time())
            self.timePointList.append(timer)

    def timerStart(self,dpiDownloadConfig):
        self.log.info("server infos(%s)",str(dpiDownloadConfig.serverinfo.keys()))
        for key,value in dpiDownloadConfig.getServerInfo().items():
            self.addTimer(value.downloadStrategy.getTimerType(),value.downloadStrategy.getTimerArg(),key)
            #timeout = value.downloadStrategy.getTimerPeriod()
            #timepoint = value.downloadStrategy.getDatetime()
        
        
