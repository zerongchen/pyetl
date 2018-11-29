#!/usr/bin/env python
#-*- coding:utf-8 -*- 
import os
import sys
import signal
import multiprocessing
import time
import bootHelp

#add project root path
sys.path.append(bootHelp.getRootPath())
sys.path.append(bootHelp.getLoadHdfsPath())
sys.path.append(bootHelp.getImportlibPath())

import importlib
from log import RunningLog,getlogger
from utils import util,fileoperation
from  utils.iniConfigParser import IniConfigParser
import application



class BootConfig(object):
    def __init__(self,modPath,bootClsname):
        self.modPath     = modPath
        self.bootClsname = bootClsname



class FrameworkConfig(object):
    def __init__(self,processnum,timerTick,logPath,logLevel,logBackupCnt,rotateSize,bootConfig, confDir, dataDir):
        self.logLevel      = logLevel
        self.logFilePath   = logPath
        self.logBackupCnt  = logBackupCnt
        self.rotateSize    = rotateSize
        self.timerTick     = timerTick
        self.processorNum  = processnum
        self.bootConfig    = bootConfig
        self.confDir       = confDir
        self.dataDir       = dataDir

    def getBootConfig(self):
        return self.bootConfig

    def getTimerTick(self):
        return self.timerTick 

    def getProcessorNum(self):
        return self.processorNum 

class TaskController(object):
    def __init__(self,frameworkconfig, logname):
        self.log              = getlogger(logname)

        # msg queue
        self.msgQueue       = multiprocessing.JoinableQueue()

        self.consumers        = []
        self.processnum       = int(frameworkconfig.getProcessorNum())
        self.producers        = {}
        # task object queue Map
        self.consumerTaskQueueMap = {}
        self.processGroups    = {}
        self.timerTick        = int(frameworkconfig.getTimerTick())
        self.frameworkconfig  = frameworkconfig

        # how long time program run
        self.runAtTime = time.time()
    

    def bindProducerConsumer(self,producerName,consumerName,consumerNum):
        self.processGroups[producerName] = consumerName
        if consumerNum >0:
            self.producers[producerName].setConsumerNum(consumerNum) 
            queue = self.consumerTaskQueueMap[consumerName]
            self.producers[producerName].setTaskQueue(queue)


    def consumerInit(self,consumerName,constructor,num_consumers,configFilename):
        bRet = True
        try:
            consumerTaskQueue = multiprocessing.JoinableQueue()
            self.consumerTaskQueueMap[consumerName] = consumerTaskQueue
            for i in xrange(num_consumers):
                consumerProcessName = consumerName + "-" + str(i)
                logFileName = os.path.join(self.frameworkconfig.logFilePath, "Consumer-" + consumerProcessName + ".log")
                RunningLog.instance().createLog(consumerProcessName, logFileName, self.frameworkconfig.logLevel,
                                                self.frameworkconfig.rotateSize, self.frameworkconfig.logBackupCnt)
                self.log.info('[%s] create consumer(%s)', __file__, consumerProcessName)
                consumer = constructor(self.msgQueue, consumerTaskQueue, getlogger(consumerProcessName),consumerProcessName,self.frameworkconfig)
                consumer.daemon = True
                consumer.name = consumerProcessName
                if not consumer.initialize(configFilename):
                    bRet = False
                    break
                else:
                    self.consumers.append(consumer)
        except:
            self.log.error("[%s] consumer(%s) init failed for %s", __file__, consumerName, util.getExceptInfo())
            bRet = False

        if not bRet:
            self.log.error("[%s] consumer(%s) init failed", __file__, consumerName)
        
        return bRet
         

    def producerInit(self,producerName,constructor,num_producers,configFilename):
        bRet = True
        try:
            for i in xrange(num_producers):
                producerProcessName = producerName + "-" + str(i)
                logFileName = os.path.join(self.frameworkconfig.logFilePath, "Producer-" + producerProcessName + ".log")
                RunningLog.instance().createLog(producerProcessName,logFileName,self.frameworkconfig.logLevel,self.frameworkconfig.rotateSize,self.frameworkconfig.logBackupCnt)
                cls = constructor(self.msgQueue,self.msgQueue,getlogger(producerProcessName),producerProcessName,self.frameworkconfig)
                cls.daemon = True
                cls.name = producerProcessName
                bRet = cls.initialize(configFilename)
                if bRet:
                    self.producers[producerName] = cls
        except:
            self.log.error("[%s] producer(%s) failed for %s", __file__, producerName, util.getExceptInfo())
            bRet = False
        if not bRet:
            self.log.info("[%s] producer(%s) Init failed", __file__, producerName)
        return bRet

            
    def toplogyInit(self):
        pass
        
    def toplogyStart(self):
        self.log.info("[%s] start all consumers", __file__)
        for consumer in self.consumers:
            consumer.start()
        self.log.info("[%s] start all producers", __file__)
        for producer in self.producers.values():
            producer.start()
        return True

    def checkTask(self):
        if self.msgQueue.empty():
            return False
        return True

    def idle(self):
        time.sleep(self.timerTick)

    def dispathTask(self):
        task = self.msgQueue.get()
        sender = task.getSender()
        rcver = task.getRecver()
        # get consumerName
        consumerName = self.processGroups[sender]
        # get consumer task queue
        consumerTaskQ    = self.consumerTaskQueueMap[consumerName]
        # submit task to consumer process
        consumerTaskQ.put(task)
        self.msgQueue.task_done()
        self.log.info("[%s] dispathTask sender:%s, rcver:%s, consumerName:%s", __file__, sender, rcver, consumerName)

    # program to exit
    def exit(self):
        if self.msgQueue != None:
            for producer in self.producers.values():
                self.log.info('[%s] TaskController(%d) to notify %s exit', __file__, os.getpid(), producer.name)
                self.msgQueue.put(producer.name+"exit")
            for consumer in self.consumers:
                self.log.info('[%s] TaskController(%d) to notify %s exit', __file__, os.getpid(), consumer.name)
                self.msgQueue.put(consumer.name+"exit")

        self.log.info('[%s] TaskController(%d) wait consumer to normal exit', __file__, os.getpid())
        for consumer in self.consumers:
            consumer.join()

        self.log.info('[%s] TaskController(%d) wait producer to normal exit', __file__, os.getpid())
        for producer in self.producers.values():
            producer.terminate()

        self.log.info('[%s] TaskController(%d) close message queue', __file__, os.getpid())
        if self.msgQueue != None:
            self.msgQueue.close()
            self.msgQueue.join_thread()

        self.log.info('[%s] TaskController(%d) close task queue', __file__, os.getpid())
        for queue in self.consumerTaskQueueMap.values():
            queue.close()
            queue.join_thread()
        self.log.info('[%s] TaskController(%d) normal exit success', __file__, os.getpid())
        os._exit(0)

# global var
global_program_exit_flag = False
global_loggerName = "Controller-DataProcessController"
global_main_program_name = "controller"

def CtrlC(a,b):
    getlogger(global_loggerName).info('[%s] program pid(%d) catch CtrlC to exit', __file__, os.getpid())
    global global_program_exit_flag
    global_program_exit_flag = True

#----------------------------
# main loop function
def work(frameworkconfig, logname):
    bRet       = False
    bootCfg = frameworkconfig.getBootConfig()
    mod = importlib.import_module(bootCfg.modPath)
    construction = getattr(mod, bootCfg.bootClsname)
    controller = construction(frameworkconfig, logname)
    bRet       = controller.toplogyInit()
    if bRet is False:
        os._exit(1)
    # start multiprocessing
    controller.toplogyStart()

    # assign global var
    global_controller = controller

    signal.signal(signal.SIGINT, CtrlC)
    signal.signal(signal.SIGTERM, CtrlC)

    # process loop deal
    try:
        global global_main_program_name
        util.setProcessName(global_main_program_name)
        getlogger(logname).info('[%s] TaskController(%d) to run', __file__, os.getpid())
        getlogger(logname).info('[%s] TaskController(%d) Program-Info : { Begin-Time: %s, Run-Time: 0.0H }',
                                __file__, os.getpid(),time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(controller.runAtTime)))
        while True:
            # to go out
            global global_program_exit_flag
            if global_program_exit_flag:
                break

            if controller.checkTask():
                controller.dispathTask()
            else:
                controller.idle()

            nowtime = time.time()
            if int(nowtime)%300 == 0:
                runLongTime = (nowtime - controller.runAtTime)/3600
                getlogger(logname).info('[%s] TaskController(%d) Program-Info : { Begin-Time: %s, Run-Time: %.2fH }',
                                        __file__, os.getpid(),time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(controller.runAtTime)), runLongTime)
    except:
        getlogger(logname).error('[%s] TaskController except exit for error %s', __file__, util.getExceptInfo())
    finally:
        getlogger(logname).info('[%s] TaskController(%d) normal exit...', __file__, os.getpid())
        controller.exit()



def daemonize(foreground):
    frameworkConfig = bootHelp.getFrameworkFile()
    if not fileoperation.isFileExist(frameworkConfig):
        print 'exited for framework config %s not exist' % (frameworkConfig)
        os._exit(1)

    #if not fileoperation.isFileExist(businessConfig):
    #    print 'exited for business config %s not exist' % (businessConfig)
    #    os._exit(1)
    try:
        IniConfigParser.instance().parse(frameworkConfig)

        modPath                =  IniConfigParser.instance().getConfVal("BOOT","modulepath")
        bootClsname            =  IniConfigParser.instance().getConfVal("BOOT","classname") 
        bootCfg                =  BootConfig(modPath,bootClsname)

        timerTick              =  IniConfigParser.instance().getConfVal("TIMER","granularity")
        processNum             =  IniConfigParser.instance().getConfVal("CONSUMER","processorNum")

        # log paraments
        backuplogNum           =  int(IniConfigParser.instance().getConfVal("LOG","backupCnt"))
        logLevel               =  IniConfigParser.instance().getConfVal("LOG","level")
        logFileSize            =  eval(IniConfigParser.instance().getConfVal("LOG","rotateSize"))
        logFilePath = bootHelp.getLogPath()

        confDir = bootHelp.getConfDirPath()
        dataDir = bootHelp.getDataDirPath()
        frameworkconfig  =  FrameworkConfig(processNum,timerTick,logFilePath,logLevel,backuplogNum,logFileSize,bootCfg,confDir,dataDir)
    except:
        print 'exited for framework config parse failed for %s' % (util.getExceptInfo())
        os._exit(1)

    logFileName = os.path.join(logFilePath, global_loggerName + '.log')
    if not fileoperation.checkOrCreateFile(logFileName):
        print 'exited for create logFileName %s failed' % (logFileName)
        os._exit(1)

    # create framework log
    RunningLog.instance().createLog(global_loggerName,logFileName,logLevel,logFileSize,backuplogNum)

    if foreground is False:
        try:
            if os.fork() >0:
                getlogger(global_loggerName).debug('[%s] parent process pid %d to exit', __file__, os.getpid())
                os._exit(0)  # exit father
        except OSError,error:
            getlogger(global_loggerName).error('[%s] fork 1 failed for %d:%s', __file__, error.errno, error.strerror)
            os._exit(1)

        #it separates the son from the father
        os.chdir('.')
        os.setsid()
        os.umask(0)
        try:
            pid = os.fork()
            if pid >0:
                getlogger(global_loggerName).debug('[%s] parent process pid %d to exit', __file__, os.getpid())
                os._exit(0)
        except OSError,error:
            getlogger(global_loggerName).error('[%s] fork 2 failed for %d:%s', __file__, error.errno,error.strerror)
            os._exit(1)

    # do real work
    work(frameworkconfig, global_loggerName)
    

if __name__=="__main__":
    if (len(sys.argv) > 1) and (sys.argv[1] == "foreground"):
        daemonize(True)
    else:
        daemonize(False)
