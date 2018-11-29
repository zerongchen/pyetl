#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback
import sys
import os
import time
import datetime
from   framework.log import getlogger
import util
import fileoperation

def checkOrCreatePartition(req):
    retVal,output = util.executeCMDWithTimeout(req,float(20))
    #getlogger().info("after exec:%s retval type:%s",req,type(retVal))
    if retVal == 127:
        getlogger().error("[%s] '%s' exc result:%s, hadoop client no install",os.path.basename(__file__),req,retVal)
        return False
    if retVal !=0 :
        getlogger().warning("%s exec result:%s",req,str(retVal))
        return False
    return True

# -1 : fault error, 1 : exist 0: not exist
def isDirExist(dirInfo, logger):
    check_dir_req = "hadoop fs -test -e %s" %(dirInfo)
    retval,output = util.executeCMD3(check_dir_req)
    if retval == 127:
        logger.error("[%s] '%s' exc result:%s, hadoop client no install",os.path.basename(__file__),check_dir_req,retval)
        return -1
    if retval == 0:
        return 1
    else:
        return 0

def createDir(dirInfo, logger):
    mkdir_dir_req  = "hadoop fs -mkdir -p %s" %(dirInfo)
    retval ,output = util.executeCMD3(mkdir_dir_req)
    if retval == 127:
        logger.error("[%s] '%s' exc result:%s, hadoop client no install", os.path.basename(__file__),mkdir_dir_req,retval)
        return False
    if retval !=0 :
        logger.error("[%s] '%s' exc failed:%s", os.path.basename(__file__), mkdir_dir_req,retval)
        return False
    return True

def fileSiz(dirInfo):
    file_size_req   = "hadoop fs -du %s |awk '{print $1}' " % (dirInfo)
    retval,sizeInfo = util.executeCMD3(file_size_req)
    if sizeInfo is None :
        getlogger().warning("[%s] %s exc failed for:%s",os.path.basename(__file__),file_size_req,util.getExceptInfo())
        return 0
    return int(sizeInfo)

def renameFile(oldFilename, newFilename, logger):
    req  = "hadoop fs -mv %s %s" % (oldFilename,newFilename)
    retVal,output= util.executeCMD3(req)
    if retVal == 127:
        getlogger().error("[%s] '%s' exc result:%s, hadoop client no install", os.path.basename(__file__),req,retVal)
        return False
    if retVal != 0:
        getlogger().warning("[%s] '%s' exec result:%s", os.path.basename(__file__),req, retVal)
        return False
    return True
    
def checkOrCreateDir(dirInfo, logger):
    return createDir(dirInfo, logger)

def fileSizCheck(dirInfo,srcSiz,retries):
    hdfsSiz = 0
    for i in range(retries):
        hdfsSiz = fileSiz(dirInfo)
        if hdfsSiz != srcSiz:
            time.sleep(0.1)
        else:
            return True,hdfsSiz
    return False,hdfsSiz


def removeFile(filenameWithPath, logger):
    req  = "hadoop fs -rm %s " % (filenameWithPath)
    retVal,output= util.executeCMD3(req)
    if retVal == 127:
        logger.error("[%s] exe:'%s' failed result:%s, hadoop client no install", os.path.basename(__file__),req,retVal)
        return False
    if retVal != 0:
        logger.error("[%s] exe:'%s' failed result:%s",os.path.basename(__file__),req,retVal)
        return False
    return True

def moveFileToHDFS(srcDir,destDir, logger):
    req = "hadoop fs -moveFromLocal %s %s" % (srcDir,destDir)
    retVal,output = util.executeCMD3(req)
    if retVal == 127:
        logger.error("[%s] '%s' exc result:%s, hadoop client no install", os.path.basename(__file__),req,retVal)
        return False
    if retVal != 0:
        return False
    return True

def storeFileToHadoop(srcDir,partitionDir, logger):
    tempfile = os.path.join(partitionDir,"." + os.path.basename(srcDir))
    #srcSize  = fileoperation.fileSize(srcDir)
    # now file not exist!
    if not moveFileToHDFS(srcDir,tempfile, None):
        # maybe partition not exist
        if not createDir(partitionDir, logger):
            return False
        if not moveFileToHDFS(srcDir, tempfile, logger):
            # maybe file is exist
            rcode = int(isDirExist(tempfile, logger))
            if rcode != 1:
                # file not is exist
                return False
            # remove file
            if not removeFile(tempfile, logger):
                return False
            if not moveFileToHDFS(srcDir, tempfile, logger):
                req = "hadoop fs -moveFromLocal %s %s" % (srcDir, tempfile)
                logger.error("[%s] exe:'%s' failed", os.path.basename(__file__), req)
                return False
    # now tmpfile success
    lastFilename = os.path.join(partitionDir,os.path.basename(srcDir))
    if not renameFile(tempfile,lastFilename, logger):
        # may be file is exist
        rcode = int(isDirExist(lastFilename, logger))
        if rcode != 1:
            # file not is exist
            return False
        # remove file
        if not removeFile(lastFilename, logger):
            return False
        if not renameFile(tempfile, lastFilename, logger):
            return False
    return True