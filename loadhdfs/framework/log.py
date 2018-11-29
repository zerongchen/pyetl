#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging.handlers
import logging

def getlogger(loggerName='Controller-DataProcessController'):
    return RunningLog.instance().getlogByName(loggerName)
    

class LogLevelMap(object):
    def __init__(self):
        self.logLevelMap = {"info":logging.INFO,"debug":logging.DEBUG,"warn":logging.WARNING}

    def getByLevelKey(self,levelKey):
        return self.logLevelMap[levelKey]

#----------------------------------------------------------#
#            Class   RunningLog                                   
#----------------------------------------------------------#
class  RunningLog:
    @classmethod    
    def instance(cls):    
        if not hasattr(cls, "_instance"):    
            cls._instance = cls()    
        return cls._instance    
  
    @classmethod    
    def initialized(cls):       
        return hasattr(cls, "_instance")


    def __init__(self):
        self.log     = None
        self.filelog = None


    def createLog(self,loggerName,logFileWithPath,logLevel,logMaxSize,backupNum):
        logMapper = LogLevelMap()
        #if not checkOrCreateFile(logFileWithPath):
        #   return False
        #Generate a logger object
        log = logging.getLogger(loggerName)
        log.setLevel(logging.DEBUG)

        #Generate log Handler,logging support FileHandler,SocketHander,SMTPHander,StreamHander
        filelog = logging.handlers.RotatingFileHandler(filename=logFileWithPath,maxBytes=logMaxSize,backupCount=backupNum)

        #Generate a log formatter
        fmtfl = logging.Formatter('%(asctime)s:%(levelname)9s:%(lineno)5d:  %(message)s')

        #Set filelog log Handler's log formatter       
        filelog.setFormatter(fmtfl)
        
        #add log Handler to logger object        
        log.addHandler(filelog)

        #Set filelog log Handler's log level
        filelog.setLevel(logMapper.getByLevelKey(logLevel))

    def getlogByName(self,name):
        return logging.getLogger(name)


    #def zero(self):
    #    self.filelog.flush()
    #    self.filelog.stream.truncate(0)
    #    self.log.info('--- Zero ---')
