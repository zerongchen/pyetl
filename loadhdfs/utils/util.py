#!/usr/bin/env python
# -*- coding: utf-8 -*-
from framework.log import getlogger
from  datetime     import datetime,date,timedelta
import traceback
import sys
import os
import time
import time
import commands
import subprocess
import socket
from setproctitle import setproctitle
###############################
#
# cmd execute interface
#
###############################
def executeCMD(cmd_str):
    try:
        resultList = os.popen(cmd_str).readlines()
        return 0
    except:
        #getlogger().warning("exec: %s failed",cmd_str)
        return 2

def executeCMD3(cmd_str):
    output = None
    try:
        status,output = commands.getstatusoutput(cmd_str)
        retval = status >>8
        return retval,output
    except:
        #getlogger().warning("exec: %s failed",cmd_str)
        return 3,output

def clearProcessResource(processor):
    if processor.stdin:
        processor.stdin.close()
    if processor.stdout:
        processor.stdout.close()
    if processor.stderr:
        processor.stderr.close()

def executeCMDWithTimeout(cmd_str,timeout,bShellOrNot=True):
    #getlogger().info("exec: %s withTimeout",cmd_str)
    begin   = time.time()
    process = subprocess.Popen(cmd_str,bufsize = 10000,stdout=subprocess.PIPE,stderr=subprocess.PIPE,close_fds=True,shell=bShellOrNot)
    childPid = process.pid
    while process.poll() is None:
        time.sleep(1)
        now = time.time()
        #getlogger().info("now:%f,begin:%f,timeout:%f",now,begin,float(timeout))
        if (now - begin) > float(timeout):
            getlogger().warning("exec: %s timeout",cmd_str)
            try:
                process.terminate()
            except Exception,e:
                pass
            finally:
                clearProcessResource(process)
                os.waitpid(childPid,0)
            return -1,None
    output = process.communicate()[0]
    retval = int(process.returncode)>>8
    clearProcessResource(process)
    #getlogger().info("retval type:%s,output:%s",type(retval),output)
    #try:
    #    process.kill()
    #except:
    #    getlogger().warning("kill: %s failed for:%s",cmd_str,getExceptInfo())
    return retval,output

    
def getRetVal(strVal):
    return int(strVal) >>8


def getExceptInfo():
    excInfo = sys.exc_info()
    return 'error %s:%s happened on line:%s' % (excInfo[0],excInfo[1],str(excInfo[2].tb_lineno))
    #traceback.print_stack()

############################
#
#  time function interface
#
#############################

def getCurrentDateStr():
    cmdStr = 'date -d "0 days ago" +"%Y%m%d"'
    dataStr = os.popen(cmdStr).readlines()[0]
    return dataStr.strip()

    #formatStr: %Y%m%d or %Y%m%d%H
def getDateTimeBySeconds(formatStr,seconds):
    localTime = time.localtime(int(seconds))
    dateInfo = time.strftime(formatStr,localTime)
    return dateInfo


def getDateByTimestamp(timestamp):
    if timestamp == None:
        return datetime.now()
    else:
        return date.fromtimestamp(timestamp)

def getDateStrByTimestamp(timestamp):
    dateInfo = getDateByTimestamp(timestamp)
    return dateInfo.strftime("%Y%m%d")

def isWeekDay(day,timestamp=None):
    dateInfo = getDateByTimestamp(timestamp)
    if dateInfo.weekday() == int(day):
            return True
    return False

def isMonthDay(day,timestamp):
    dateInfo = getDateByTimestamp(timestamp)
    if dateInfo.day == int(day):
        return True
    return False

def getLaskWeek(timestamp):
    dateInfo       = getDateByTimestamp(timestamp)
    endDate        = dateInfo - timedelta(days=1)
    endDateStr     = endDate.strftime("%Y%m%d")
    startDate      = dateInfo - timedelta(days=7)
    startDateStr   = startDate.strftime("%Y%m%d")
    return startDateStr,endDateStr

def getLaskMonth(timestamp):
    dateInfo       = getDateByTimestamp(timestamp)
    endDate        = dateInfo - timedelta(days=1)
    endDateStr     = endDate.strftime("%Y%m%d") 
    startDate      = datetime(dateInfo.year,dateInfo.month-1,1)
    startDateStr   = startDate.strftime("%Y%m%d")
    return startDateStr,endDateStr


################################
#
#  socket function interface
#
################################
def getLocalIP():
    return socket.gethostbyname(socket.gethostname())

def setProcessName(processname):
    setproctitle("etl_" + processname)













    





    






      




            

    
       
        
